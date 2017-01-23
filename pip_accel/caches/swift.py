"""
Swift cache backend.

"""

# Standard library modules.
import logging
import os

# Modules included in our package.
from pip_accel.caches import AbstractCacheBackend
from pip_accel.exceptions import CacheBackendDisabledError
from pip_accel.utils import AtomicReplace, makedirs

from swiftclient import Connection
from swiftclient.exceptions import ClientException

# Initialize a logger for this module.
logger = logging.getLogger(__name__)

class SwiftCacheBackend(AbstractCacheBackend):
    """The swift cache backend stores Python distribution archives in a swift bucket."""

    PRIORITY = 20

    def __init__(self, config):
        super(SwiftCacheBackend, self).__init__(config)
        self._connection = None

    def get(self, filename):
        """
        Check if a distribution archive exists in the local cache.

        :param filename: The filename of the distribution archive (a string).
        :returns: The pathname of a distribution archive on the local file
                  system or :data:`None`.
        """
        pathname = os.path.join(self.config.binary_cache, filename)
        if os.path.isfile(pathname):
            logger.debug("Distribution archive exists in local cache (%s).", pathname)
            return pathname

        try:
            makedirs(os.path.dirname(pathname))
            with AtomicReplace(pathname) as temporary_file:
                name = self.get_cache_key(filename)
                logger.info("Trying go get distribution archive from swift container: %s", name)
                headers, content = self.connection.get_object(self.container_name, name)
                with open(temporary_file, 'wb') as o:
                    o.write(content)

            logger.debug("Finished downloading distribution archive from Swift bucket.")
            return pathname
        except ClientException:
            return None

    def put(self, filename, handle):
        """
        Store a distribution archive in the local cache.

        :param filename: The filename of the distribution archive (a string).
        :param handle: A file-like object that provides access to the
                       distribution archive.
        """
        name = self.get_cache_key(filename)
        self.connection.put_object(self.container_name, name,
                                   contents=handle,
                                   content_type='application/binary')
        logger.debug("Finished caching distribution archive in Swift bucket.")

    def get_cache_key(self, filename):
        """
        Compose an Swift cache key based on :attr:`.swift_cache_prefix` and the given filename.

        :param filename: The filename of the distribution archive (a string).
        :returns: The cache key for the given filename (a string).
        """
        return '/'.join(filter(None, [self.swift_cache_prefix, filename]))

    @property
    def connection(self):
        try:
            if not self._connection:
                self._connection = Connection(user=self.username,
                                              key=self.password,
                                              authurl=self.auth_url,
                                              auth_version=self.auth_version,
                                              os_options=self.os_options
                                              )
        except ClientException as e:
            raise CacheBackendDisabledError(e.msg)
        return self._connection

    @property
    def swift_cache_prefix(self):
        return self.config.get(property_name="swift_cache_prefix",
                               environment_variable="PIP_SWIFT_CACHE_PREFIX",
                               configuration_option="swift-cache-prefix",
                               default='')

    @property
    def container_name(self):
        return self.config.get(property_name="swift_cache_container_name",
                               environment_variable="PIP_SWIFT_CACHE_CONTAINER_NAME",
                               configuration_option="swift-cache-container-name",
                               default='')

    @property
    def username(self):
        return self._swift_property('username')

    @property
    def password(self):
        return self._swift_property('password')

    @property
    def auth_url(self):
        return self._swift_property('auth_url')

    @property
    def auth_version(self):
        return self._swift_property('identity_api_version', '3')

    @property
    def os_options(self):
        options = {}
        for key in ['auth_token', 'tenant_name', 'tenant_id', 'user_id', 'username', 'user_domain_name',
                    'user_domain_id', 'project_name', 'project_id', 'project_domain_name', 'project_domain_id',
                    'region_name']:
            value = self._swift_property(key)
            if value is not None:
                options[key] = value

        return options

    def _swift_property(self, name, default=None):
        return self.config.get(property_name="os_{}".format(name),
                               environment_variable="OS_{}".format(name.upper()),
                               configuration_option="os-{}".format(name.replace("_", "-")),
                               default=default)

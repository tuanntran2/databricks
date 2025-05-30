# Databricks notebook source
# MAGIC %md
# MAGIC # Helper class to access data lake storage containers
# MAGIC  

# COMMAND ----------

class Access:
    def __init__(self, storage_account_name: str):
        self._storage_account_name = storage_account_name

    def is_adls_mounted(self, mount_point: str) -> bool:
        mounts = [mount.mountPoint for mount in dbutils.fs.mounts()]
        return mount_point in mounts

    def mount(self, container_name: str) -> str:
        mount_point = f"/mnt/{container_name}"
        if self.is_adls_mounted(mount_point):
            return mount_point

        dbutils.fs.mount(
            source = f"wasbs://{container_name}@{self._storage_account_name}.blob.core.windows.net",
            mount_point=mount_point,
            extra_configs=self._create_extra_config(container_name)
        )
        return mount_point

    def unmount_adls(self, mount_point: str):
        if self.is_adls_mounted(mount_point):
            dbutils.fs.unmount(mount_point)

    def _create_extra_config(self, container_name: str) -> str:
        raise NotImplemented


class AccessKey(Access):
    def __init__(self, storage_account_name: str, access_key: str):
        super().__init__(storage_account_name)
        self._access_key = access_key

    def _create_extra_config(self, container_name: str) -> str:
        return {
            f"fs.azure.account.key.{self._storage_account_name}.blob.core.windows.net": self._access_key
        }
        

class SASToken(Access):
    def __init__(self, storage_account_name: str, sas_token: str):
        super().__init__(storage_account_name)
        self._sas_token = sas_token

    def _create_extra_config(self, container_name: str) -> str:
        return {
            f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": self._sas_token
        }


class ServicePrincipal(Access):
    def __init__(self, storage_account_name: str, client_id: str, tenant_id : str, client_secret):
        super().__init__(storage_account_name)
        self._client_id = client_id
        self._tenant_id = tenant_id
        self._client_secret = client_secret

    def _create_extra_config(self, container_name: str) -> str:
        return {
            f"fs.azure.account.auth.type": "OAuth",
            f"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            f"fs.azure.account.oauth2.client.id": self._client_id,
            f"fs.azure.account.oauth2.client.secret": self._client_secret,
            f"fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{self._tenant_id}/oauth2/token"
        }

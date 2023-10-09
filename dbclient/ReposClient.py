import json
from dbclient import *
import os
from copy import deepcopy

class ReposClient(dbclient):

    def __init__(self, configs, checkpoint_service):
        super().__init__(configs)
        self._checkpoint_service = checkpoint_service
        self.groups_to_keep = configs.get("groups_to_keep", False)
        self.skip_missing_users = configs['skip_missing_users']
        self.hipaa = configs.get('hipaa', False)
        self.bypass_secret_acl = configs.get('bypass_secret_acl', False)

    def _get_all_repos(self):
        repos_list = self.get('/repos')
        return repos_list.get("repos")

    def log_repos_list(self, log_file="repos.log"):
        repo_log_file_path = self.get_export_dir() + log_file
        repos_list = self._get_all_repos()
        with open(repo_log_file_path, "w", encoding="utf-8") as fp:
            for x in repos_list:
                fp.write(json.dumps(x))
                fp.write("\n")

    def log_repos_acl(self, log_file="repos_acl.log"):
        repo_acl_log_file_path = self.get_export_dir() + log_file
        repos_list = self._get_all_repos()
        with open(repo_acl_log_file_path, "w", encoding="utf-8") as fp:
            for repo in repos_list:
                repo_acl = self.get("/permissions/repos/{}".format(repo.get("id")))
                repo_acl.update({"id": repo.get("id")})
                fp.write(json.dumps(repo_acl))
                fp.write("\n")
    
    def import_repos_with_acl(self, log_file="repos.log", acl_log_file="repos_acl.log"):
        repo_log_file_path = self.get_export_dir() + log_file
        repo_acl_log_file_path = self.get_export_dir() + acl_log_file
        source_destination_repo_id_map = {}
        with open(repo_log_file_path, 'r', encoding="utf-8") as fp:
            for line in fp:
                data = json.loads(line)
                repo_create = self.post("/repos", json_params=data)
                source_destination_repo_id_map[data.get("id")] = repo_create.get("id")
        
        with open(repo_acl_log_file_path, 'r', encoding="utf-8") as fp:
            for line in fp:
                data = json.loads(line)
                access_control_list = []
                for access in data.get("access_control_list"):
                    if "user_name" in access:
                        current_data = {"user_name": access.get("user_name")}
                    elif "group_name" in access:
                        current_data = {"group_name": access.get("group_name")}
                    for perm in access.get("all_permissions"):
                        current_data["permission_level"] = perm.get("permission_level")
                        break
                    current_data["all_permissions"] = access.get("all_permissions")
                    access_control_list.append(current_data)
                json_params = {"access_control_list": access_control_list}
                print(json_params)
                repo_id = source_destination_repo_id_map[data.get("id")]
                api_response = self.put("/permissions/repos/{}".format(repo_id), json_params=json_params)
                print(api_response)

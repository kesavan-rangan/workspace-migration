import json
from dbclient import *
import os

class LibraryClient(dbclient):

    def __init__(self, configs, checkpoint_service):
        super().__init__(configs)
        self._checkpoint_service = checkpoint_service
        self.groups_to_keep = configs.get("groups_to_keep", False)
        self.skip_missing_users = configs['skip_missing_users']
        self.hipaa = configs.get('hipaa', False)
        self.bypass_secret_acl = configs.get('bypass_secret_acl', False)

    def get_cluster_list(self, alive=True):
        """ Returns an array of json objects for the running clusters. Grab the cluster_name or cluster_id """
        cl = self.get("/clusters/list", print_json=False)
        if alive:
            running = filter(lambda x: x['state'] == "RUNNING", cl['clusters'])
            return list(running)
        else:
            return cl['clusters']

    # def log_library_details(self, log_file='lib_details.log'):
    #     libs_log = self.get_export_dir() + log_file
    #     all_libs = self.get('/libraries/list', version='1.2')
    #     with open(libs_log, "w", encoding="utf-8") as fp:
    #         for x in all_libs.get('elements', None):
    #             lib_details = self.get('/libraries/status?libraryId={0}'.format(x['id']), version='1.2')
    #             fp.write(json.dumps(lib_details) + '\n')

    # def log_cluster_libs(self, cl_log_file='attached_cluster_libs.log'):
    #     cl_lib_log = self.get_export_dir() + cl_log_file
    #     cl = self.get_cluster_list(False)
    #     with open(cl_lib_log, "w", encoding="utf-8") as fp:
    #         for x in cl:
    #             cid = x['cluster_id']
    #             libs = self.get("/libraries/cluster-status?cluster_id={0}".format(cid))
    #             fp.write(json.dumps(libs))
    #             fp.write("\n")

    def get_all_clusters(self):
        all_clusters = self.get('/clusters/list')
        return all_clusters.get('clusters')

    def get_cluster_info_by_name(self, clusters_list, cluster_name: str):
        for cluster in clusters_list:
            if cluster.get("cluster_name") == cluster_name:
                return cluster 
    
    def import_dbfs_libraries(self, log_file="lib_details.log", dbfs_libraries_path="dbfs_libraries"):
        libraries_log_file = os.path.join(self.get_export_dir(), log_file)
        dbfs_libraries_to_import = []
        with open(libraries_log_file, 'r', encoding="utf-8") as fp:
            for library in fp:
                lib_properties = json.loads(library)
                if lib_properties.get('library_statuses'):
                    for lib_item in lib_properties.get('library_statuses'):
                        library_info = lib_item.get('library')
                        if 'jar' in library_info:
                            if library_info.get('jar') not in dbfs_libraries_to_import:
                                dbfs_libraries_to_import.append(library_info.get('jar'))
        
        for dbfs_libs in dbfs_libraries_to_import:
            lib_name = os.path.basename(dbfs_libs)
            lib_path = os.path.join(self.get_export_dir(), dbfs_libraries_path, lib_name)
            split_path = os.path.split(dbfs_libs)
            with open(lib_path, "rb") as fp:
                lib_file_data = fp.read().decode('utf-8')

                create_dir = {"path": split_path[0]}
                cl = self.post("/dbfs/mkdirs", json_params=create_dir)

                content_info = {"path": dbfs_libs, "contents": lib_file_data, "overwrite": True}
                cl = self.post("/dbfs/put", json_params=content_info)


    def import_libraries(self, cluster_log_file="clusters.log", log_file="attached_cluster_libs.log"):
        self.import_dbfs_libraries()
        #  Getting all cluster, and create a map with the name and the ID
        destination_all_clusters = self.get_all_clusters()
        cluster_log_files = os.path.join(self.get_export_dir(), cluster_log_file)
        source_dest_cluster_ids_map = {}
        with open(cluster_log_files, 'r', encoding="utf-8") as fp:
            for line in fp:
                source_cluster_info = json.loads(line)
                destination_cluster_info = self.get_cluster_info_by_name(destination_all_clusters, source_cluster_info.get("cluster_name"))
                source_dest_cluster_ids_map[source_cluster_info.get("cluster_id")] = destination_cluster_info.get("cluster_id")


        libraries_log = os.path.join(self.get_export_dir(), log_file)
        with open(libraries_log, 'r', encoding="utf-8") as fp:
            for line in fp:
                data = json.loads(line)
                if "library_statuses" in data:
                    payload = {"cluster_id": source_dest_cluster_ids_map[data.get("cluster_id")]}
                    all_libraries = []
                    for library in data.get("library_statuses"): 
                        all_libraries.append(library.get("library"))
                    payload["libraries"] = all_libraries
                    lib_resp = self.post('/libraries/install', json_params=payload)


    def export_dbfs_libaries(self, dbfs_path, dbfs_libraries_path="dbfs_libraries"):
        dbfs_lib_log_path = os.path.join(self.get_export_dir() + dbfs_libraries_path)
        if not os.path.exists(dbfs_lib_log_path):
            os.makedirs(dbfs_lib_log_path)
        dbfs_file_name = os.path.basename(dbfs_path)
        lib_req = self.get('/dbfs/read?path={}'.format(dbfs_path))
        with open(os.path.join(dbfs_lib_log_path, dbfs_file_name), "wb") as fp:
            fp.write(lib_req.get("data").encode('utf-8'))
    
    def log_library_details(self, log_file='lib_details.log'):
        libs_log = self.get_export_dir() + log_file
        all_libs = self.get('/libraries/all-cluster-statuses')
        with open(libs_log, "w", encoding="utf-8") as fp:
            for x in all_libs.get('statuses', None):
                lib_details = self.get('/libraries/cluster-status?cluster_id={0}'.format(x['cluster_id']))
                fp.write(json.dumps(lib_details) + '\n')
        for lib in all_libs.get('statuses'):
            if "library_statuses" in lib:
                for lib_2 in lib.get('library_statuses'):
                    lib_details = lib_2.get("library")
                    if "jar" in lib_details:
                        self.export_dbfs_libaries(lib_details.get("jar"))

    def log_cluster_libs(self, cl_log_file='attached_cluster_libs.log'):
        cl_lib_log = self.get_export_dir() + cl_log_file
        cl = self.get_cluster_list(False)
        with open(cl_lib_log, "w", encoding="utf-8") as fp:
            for x in cl:
                cid = x['cluster_id']
                libs = self.get("/libraries/cluster-status?cluster_id={0}".format(cid))
                fp.write(json.dumps(libs))
                fp.write("\n")

# -*- coding: utf-8 -*-
import os
import simplejson as json
import datetime
from six.moves.urllib.parse import quote
from dremio_client.error import DremioException


def fetch(client, base):
    dataset_paths = []
    parent = None
    start = client.data
    if base is not None:
        for b in base:
            start = start[b.replace('"', "").replace(" ", "_").replace("-", "_").replace("@", "").replace(".", "_")].get()
    if start.meta.entityType == "source" or start.meta.entityType == "folder":
        parent = client.simple().catalog_item(None, start.meta.path)
        dataset_paths = recurse_child_pdss(client, parent["children"])
    return dataset_paths, parent


def recurse_child_pdss(client, children):
    dataset_paths = []
    for child in children:
        if child["type"] != "DATASET" and child["type"] != "CONTAINER":
            continue
        if child["type"] == "DATASET":
            dataset_paths.append(child["path"])
        else:
            object = client.simple().catalog_item(None, child["path"])
            d = recurse_child_pdss(client, object["children"])
            dataset_paths.extend(d)
    return dataset_paths


def fetch_object_paths(client, base=None, baselen=0):
    object_paths = []
    start = client.data
    if baselen > 0:
        for b in base:
            start = start[b.replace('"', "").replace(" ", "_").replace("-", "_").replace("@", "").replace(".", "_")].get()
        if start.meta.entityType == "space" or start.meta.entityType == "folder":
            parent = client.simple().catalog_item(None, start.meta.path)
            if start.meta.entityType == "space":
                object_paths.append([parent["name"]])
            else:
                object_paths.append(parent["path"])
            object_paths.extend(recurse_child_objects(client, parent["children"]))
    else:
        catalog = client.simple().catalog()
        for entity in catalog["data"]:
            if entity["type"] == "CONTAINER" and entity["containerType"] == "SPACE":
                object_paths.append(entity["path"])
                object = client.simple().catalog_item(None, entity["path"])
                d = recurse_child_objects(client, object["children"])
                object_paths.extend(d)
    return object_paths


def recurse_child_objects(client, children):
    object_paths = []
    for child in children:
        if child["type"] != "DATASET" and child["type"] != "CONTAINER":
            continue
        object_paths.append(child["path"])
        if child["type"] == "CONTAINER":
            object = client.simple().catalog_item(None, child["path"])
            d = recurse_child_objects(client, object["children"])
            object_paths.extend(d)
    return object_paths


def build_acl_defs(acl_file):
    acl_defs = json.load(acl_file)
    return acl_defs


def build_default_acl(group_on_acl_empty,user_on_acl_empty):
    acl = {"accessControlList": {}}
    if user_on_acl_empty:
        user = [
                        {
                            "id": user_on_acl_empty,
                            "permissions": ["READ","WRITE"]
                        }
                    ]
        acl["accessControlList"]["users"] = user
    if group_on_acl_empty:
        group = [
            {
                "id": group_on_acl_empty,
                "permissions": ["READ", "WRITE"]
            }
        ]
        acl["accessControlList"]["groups"] = group
    return acl


def submit(client, objs):
    bad = []
    for obj in objs:
        try:
            client.simple().update_catalog(quote(obj['id'], safe=''), obj)
        except DremioException, e:
            print(e)
            bad.append(obj)
    return bad


def delete_pds_acls(client, pdss_with_acls, out=lambda x: x):
    dirty_datasets = []
    for pds in pdss_with_acls:
        pds['accessControlList'] = {}
        dirty_datasets.append(pds)
        out("Deleted ACLs for PDS {}".format("/".join(pds["path"])))

    last_run = len(dirty_datasets) + 1
    bad = dirty_datasets
    while len(bad) < last_run:
        last_run = len(bad)
        bad = submit(client, bad)
    if len(bad) > 0:
        out("Failed to commit the following {} items:\n{}".format(
            len(bad),
            "\n".join('.'.join('"{}"'.format(j) for j in i["path"]) for i in bad)))
    else:
        out("Commit complete")


def update_object_acl(db_object, acl_defs, default_acl, out=lambda x: x):
    dirty_objects = []
    has_acl = False
    for acls in acl_defs['entities']:
        # check if an object has an entry in the ACLs set. Object can be PDS or database
        if acls['entityPath'] == (db_object["name"] if db_object["entityType"] == "space" else db_object["path"]) : #db_object["path"]:
            has_acl = True;
            # check if any ACLs are different
            if acls['accessControlList'] != db_object['accessControlList']:
                # they are different so update the object and add it to the set to be committed
                db_object["accessControlList"] = acls['accessControlList']
                dirty_objects.append(db_object)
                out("Updated ACLs for {}".format(db_object["name"] if db_object["entityType"] == "space" else "/".join(db_object["path"])))
    # if no ACLs found in acl_defs for object
    if has_acl == False:
        # check if the current ACLs for the object are not empty and contain anything other than the default acl
        if db_object["accessControlList"] and ("users" in db_object["accessControlList"] or "groups" in db_object["accessControlList"]):
            if "version" in db_object["accessControlList"]:
                del db_object["accessControlList"]["version"]
            if db_object["accessControlList"] != default_acl["accessControlList"]:
                # reset the ACLS for the object to default ACL and add the object to the set to be committed
                db_object["accessControlList"] = default_acl["accessControlList"]
                dirty_objects.append(db_object)
                out("Revoked ACLs for {}".format(db_object["name"] if db_object["entityType"] == "space" else "/".join(db_object["path"])))
        else:
            # the ACLs for the object are empty. If the default ACL is not empty, then need to set to the default ACL
            if default_acl["accessControlList"]:
                db_object["accessControlList"] = default_acl["accessControlList"]
                dirty_objects.append(db_object)
                out("ACLs for {} are empty, setting to default ACL".format(db_object["name"] if db_object["entityType"] == "space" else "/".join(db_object["path"])))
    return dirty_objects


def update_acl(client, acl_defs, datasets, source_folder, source_only, default_acl, out=lambda x: x):
    dirty_datasets = []
    if source_folder["entityType"] == "source":
        for child in source_folder["children"]:
            if child["type"] == "CONTAINER": # we have a database
                try:
                    out("Processing {}".format("/".join(child["path"])))
                    db_object = client.simple().catalog_item(None, child["path"])
                    d = update_object_acl(db_object, acl_defs, default_acl, out)
                    dirty_datasets.extend(d)
                except Exception, e:
                    print(e)
                    out("Unable to process database {}".format("/".join(child["path"])))
    elif source_folder["entityType"] == "folder":
        try:
            out("Processing {}".format("/".join(source_folder["path"])))
            db_object = client.simple().catalog_item(None, source_folder["path"])
            d = update_object_acl(db_object, acl_defs, default_acl, out)
            dirty_datasets.extend(d)
        except Exception, e:
            print(e)
            out("Unable to process database {}".format("/".join(source_folder["path"])))
    if not source_only: #process PDSs
        for dataset_path in datasets:
            try:
                out("Processing {}".format("/".join(dataset_path)))
                pre_time = datetime.datetime.now()
                pds = client.simple().catalog_item(None, dataset_path)
                post_time = datetime.datetime.now()
                difference = post_time - pre_time
                out("GET /catalog/by-path/ took {} for {}".format(difference, "/".join(pds["path"])))
                d = update_object_acl(pds, acl_defs, default_acl, out)
                dirty_datasets.extend(d)
            except Exception, e:
                print(e)
                out("Unable to process PDS {}".format("/".join(dataset_path)))
    last_run = len(dirty_datasets) + 1
    bad = dirty_datasets
    while len(bad) < last_run:
        last_run = len(bad)
        bad = submit(client, bad)
    if len(bad) > 0:
        out("Failed to commit the following {} items:\n{}".format(
            len(bad),
            "\n".join('.'.join('"{}"'.format(j) for j in i["path"]) for i in bad)))
    else:
        out("Commit complete")


def update_space_acl(client, acl_defs, datasets, default_acl, out=lambda x: x):
    dirty_datasets = []
    for dataset_path in datasets:
        try:
            out("Processing {}".format("/".join(dataset_path)))
            pre_time = datetime.datetime.now()
            obj = client.simple().catalog_item(None, dataset_path)
            post_time = datetime.datetime.now()
            difference = post_time - pre_time
            out("GET /catalog/by-path/ took {} for {}".format(difference, (obj["name"] if obj["entityType"] == "space" else "/".join(obj["path"]))))
            d = update_object_acl(obj, acl_defs, default_acl, out)
            dirty_datasets.extend(d)
        except Exception, e:
            print(e)
            out("Unable to process {}".format("/".join(dataset_path)))
    last_run = len(dirty_datasets) + 1
    bad = dirty_datasets
    while len(bad) < last_run:
        last_run = len(bad)
        bad = submit(client, bad)
    if len(bad) > 0:
        out("Failed to commit the following {} items:\n{}".format(
            len(bad),
            "\n".join('.'.join('"{}"'.format(j) for j in i["path"]) for i in bad)))
    else:
        out("Commit complete")


def update_acls_to_folder(client, datasets, source_folder, default_acl, del_pds_acls, out=lambda x: x):
    dirty_folders = []
    pdss_with_acls = []
    aclSuperset = {}
    for dataset_path in datasets:
        try:
            out("Processing {}".format("/".join(dataset_path)))
            pre_time = datetime.datetime.now()
            pds = client.simple().catalog_item(None, dataset_path)
            post_time = datetime.datetime.now()
            difference = post_time - pre_time
            out("GET /catalog/by-path/ took {} for {}".format(difference, "/".join(pds["path"])))
            if pds['accessControlList']:
                pdss_with_acls.append(pds)
                # look through each accessControlList entry in the PDS and see if the user/group exists in aclSuperset
                for aclEntry in pds['accessControlList']:
                    if aclEntry not in aclSuperset:
                        aclSuperset[aclEntry] = []
                        # user/group entry exists in aclSuperset, now lets look at ACLs for each specific user and group and compare to what's in the superset
                    for pdsAclEntry in pds['accessControlList'][aclEntry]:
                        isAclIdInSuperset = False
                        for supersetAclEntry in aclSuperset[aclEntry]:
                            if pdsAclEntry['id'] == supersetAclEntry['id']:
                                isAclIdInSuperset = True
                                supersetPermissions = list(set(pdsAclEntry['permissions']) | set(supersetAclEntry['permissions']))
                                supersetAclEntry['permissions'] = supersetPermissions
                        if not isAclIdInSuperset:
                            aclSuperset[aclEntry].append(pdsAclEntry)
        except Exception, e:
            print(e)
            out("Unable to process PDS {}".format("/".join(dataset_path)))
    out("Superset ACLs: {}".format(aclSuperset))
    # if the ACLs for the superset are empty and if the default ACL is not empty, then need to set to the default ACL into the source folder
    if len(aclSuperset) > 0:
        source_folder["accessControlList"] = aclSuperset
        dirty_folders.append(source_folder)
        out("Updated ACL superset for Folder {}".format("/".join(source_folder["path"])))
    else:
        if default_acl["accessControlList"]:
            source_folder["accessControlList"] = default_acl["accessControlList"]
            dirty_folders.append(source_folder)
            out("ACL superset for Folder {} is empty, setting to default ACL".format("/".join(source_folder["path"])))
        else:
            out("ACL superset for Folder {} is empty and no default ACL defined. Not updating.".format("/".join(source_folder["path"])))

    bad = submit(client, dirty_folders)
    if len(bad) > 0:
        out("Failed to commit the following {} items:\n{}".format(
            len(bad),
            "\n".join('.'.join('"{}"'.format(j) for j in i["path"]) for i in bad)))
    else:
        out("Commit complete")
        if del_pds_acls:
            out("Deleting PDS ACLs")
            delete_pds_acls(client, pdss_with_acls, out)


def report_acl(client, base, acl_defs, datasets, report_path, default_acl, out=lambda x: x):
    try:
        os.makedirs(report_path)
        out("{} created".format(report_path))
    except:
        pass
    filename = "acl_report_" + "-".join(base) + "_" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    jf = os.path.join(report_path, filename) + ".json"
    report_file = open(jf, 'a')
    count = 0
    for dataset_path in datasets:
        has_acl = False;
        try:
            out("Processing {}".format("/".join(dataset_path)))
            pre_time = datetime.datetime.now()
            pds = client.simple().catalog_item(None, dataset_path)
            post_time = datetime.datetime.now()
            difference = post_time - pre_time
            out("GET /catalog/by-path/ took {} for {}".format(difference, "/".join(pds["path"])))
            for acls in acl_defs['entities']:
                # check if a dataset has an entry in the ACLs set.
                if acls['entityPath'] == pds["path"]:
                    has_acl = True;
                    # check if any ACLs are different
                    if acls['accessControlList'] != pds['accessControlList']:
                        # they are different so update the dataset and add it to the set to be committed
                        out("ACLs mismatch for {}".format("/".join(pds["path"])))
                        acl_dict = {'id': pds["id"], 'path': pds["path"], 'aclReport': "ACLs mismatch for {}".format("/".join(pds["path"]))}
                        report_file.write(json.dumps(acl_dict) + '\n')
                        count += 1
            # if no ACLs found in acl_defs for dataset
            if has_acl == False:
                # check if the current ACLs for the dataset are not empty and contain anything other than the default acl
                if pds["accessControlList"]:
                    if pds["accessControlList"] != default_acl["accessControlList"]:
                        out("ACLs must be revoked for {}".format("/".join(pds["path"])))
                        acl_dict = {'id': pds["id"], 'path': pds["path"],
                                    'aclReport': "ACLs must be revoked for {}".format("/".join(pds["path"]))}
                        report_file.write(json.dumps(acl_dict) + '\n')
                        count += 1
                else:
                    # the ACLs for the PDS are empty. If the default ACL is not empty, then need to set to the default ACL
                    if default_acl["accessControlList"]:
                        out("ACLs for PDS {} are empty, must set to default ACL".format("/".join(pds["path"])))
                        acl_dict = {'id': pds["id"], 'path': pds["path"],
                                    'aclReport': "ACLs for PDS {} are empty, must set to default ACL".format("/".join(pds["path"]))}
                        report_file.write(json.dumps(acl_dict) + '\n')
                        count += 1
        except:
            out("Unable to process PDS {}".format("/".join(dataset_path)))
            acl_dict = {'id': "", 'path': pds["path"],
                        'aclReport': "Unable to process PDS {}".format("/".join(dataset_path))}
            report_file.write(json.dumps(acl_dict) + '\n')
    report_file.close()
    out("Report complete. {} PDSs need updating.".format(count))


def dump_acl(client, datasets, dump_path, base, out=lambda x: x):
    try:
        os.makedirs(dump_path)
        out("{} created".format(dump_path))
    except:
        pass
    filename = "acl_dump_" + "-".join(base) + "_" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    jf = os.path.join(dump_path, filename) + ".json"
    acl_file = open(jf, 'a')
    for dataset_path in datasets:
        try:
            out("Processing {}".format("/".join(dataset_path)))
            pre_time = datetime.datetime.now()
            pds = client.simple().catalog_item(None, dataset_path)
            post_time = datetime.datetime.now()
            difference = post_time - pre_time
            out("GET /catalog/by-path/ took {} for {}".format(difference, "/".join(pds["path"])))
            acl_dict = {'id': pds["id"], 'path': pds["path"], 'accessControlList' : pds["accessControlList"]}
            acl_file.write(json.dumps(acl_dict) + '\n')
        except:
            out("Unable to process PDS {}".format("/".join(dataset_path)))
    acl_file.close()
    out("Dump complete. See file {} for details.".format(jf))


def dump_space_acl(client, objects, dump_path, base, out=lambda x: x, include_vds=False):
    try:
        os.makedirs(dump_path)
        out("{} created".format(dump_path))
    except:
        pass
    filename = "space_acl_dump_" + "-".join(base) + "_" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    jf = os.path.join(dump_path, filename) + ".json"
    acl_file = open(jf, 'a')
    for object_path in objects:
        try:
            #pre_time = datetime.datetime.now()
            obj = client.simple().catalog_item(None, object_path)
            #post_time = datetime.datetime.now()
            #difference = post_time - pre_time
            #out("GET /catalog/by-path/ took {} for {}".format(difference, "/".join(obj["path"])))
            if not include_vds and obj["entityType"] == "dataset":
                continue
            out("Processing {}".format("/".join(object_path)))
            acl_dict = {'id': obj["id"], 'path': [obj["name"]] if obj["entityType"] == 'space' else obj["path"], 'entityType': obj["entityType"], 'accessControlList': obj["accessControlList"]}
            acl_file.write(json.dumps(acl_dict) + '\n')
        except Exception as e:
            out(e)
            out("Unable to process {}".format("/".join(object_path)))
    acl_file.close()
    out("Dump complete. See file {} for details.".format(jf))

# -*- coding: utf-8 -*-
"""Console script for dremio_acl."""
import os
import sys
import click
import requests

from dremio_client.dremio_client import DremioClient
from dremio_client.conf.config_parser import build_config
from .acl import fetch, fetch_object_paths, build_acl_defs, build_default_acl,  report_acl, update_acl, update_space_acl, dump_acl, dump_space_acl, update_acls_to_folder

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

@click.group()
@click.option("--config", type=click.Path(exists=True, dir_okay=True, file_okay=False), help="Custom config file.")
@click.option("-h", "--hostname", help="Hostname if different from config file")
@click.option("-p", "--port", type=int, help="Hostname if different from config file")
@click.option("--ssl", is_flag=True, default=False, help="Use SSL if different from config file")
@click.option("-u", "--username", help="username if different from config file")
@click.option("--password", help="password if different from config file")
@click.option("--skip-verify", is_flag=True, help="skip verification of ssl cert")
@click.pass_context
def cli(ctx, config, hostname, port, ssl, username, password, skip_verify):
    """
    Use dremio_acl to interact with Dremio's REST API to report on or update PDS ACLs
    """
    if config:
        os.environ["DREMIO_CLIENTDIR"] = config
    ctx.obj = dict()
    if hostname:
        ctx.obj["hostname"] = hostname
    if port:
        ctx.obj["port"] = port
    if ssl:
        ctx.obj["ssl"] = ssl
    if username:
        ctx.obj["auth.username"] = username
    if password:
        ctx.obj["auth.password"] = password
    if skip_verify:
        ctx.obj["verify"] = not skip_verify


@cli.group()
@click.pass_obj
def update(args):
    pass


@cli.group()
@click.pass_obj
def report(args):
    pass


@cli.group()
@click.pass_obj
def dump(args):
    pass


@update.command()
@click.argument("base", nargs=-1, required=True)
@click.option("-g", "--group-on-acl-empty", "group_on_acl_empty", required=False, show_default=False, default=None, help="Optional group to set ACL for if a PDS ACL list is empty")
@click.option("-u", "--user-on-acl-empty", "user_on_acl_empty", required=False, show_default=False, default=None, help="Optional user to set ACL for if a PDS ACL list is empty")
@click.option("-d", "--delete-pds-acls", "delete_pds_acls", is_flag=True, default=False, show_default=False, required=False, help="Flag to delete PDS ACLs once the data source folder is updated")
@click.pass_obj
def acls_to_folder(args, group_on_acl_empty, user_on_acl_empty, delete_pds_acls, base):
    """
        BASE: base directory of data source folder in Dremio for which to generate superset of ACLs. Space separated. e.g. to start at a folder within a source: MYSOURCE MYDB
    """
    client = DremioClient(build_config(args))
    default_acl = build_default_acl(group_on_acl_empty, user_on_acl_empty)
    ds, source_folder = fetch(client, base)
    update_acls_to_folder(client, ds, source_folder, default_acl, delete_pds_acls, click.echo)


@update.command()
@click.argument("base", nargs=-1, required=True)
@click.option("-a", "--acl_file", "acl_file", required=True, type=click.File(), help="Path to a file containing the ACL definitions")
@click.option("-g", "--group-on-acl-empty", "group_on_acl_empty", required=False, show_default=False, default=None, help="Optional group to set ACL for if a PDS ACL list is empty")
@click.option("-u", "--user-on-acl-empty", "user_on_acl_empty", required=False, show_default=False, default=None, help="Optional user to set ACL for if a PDS ACL list is empty")
@click.option("-s", "--source-only", "source_only", is_flag=True, default=False, show_default=False, required=False, help="Flag to only set ACLs at database level, omit setting PDS ACLs")
@click.pass_obj
def acl(args, acl_file, group_on_acl_empty, user_on_acl_empty, source_only, base):
    """
        BASE: base directory in Dremio where to start applying ACLs to. Space separated. e.g. to start at a db within a source: MYSOURCE MYDB
    """
    client = DremioClient(build_config(args))
    acl_defs = build_acl_defs(acl_file)
    default_acl = build_default_acl(group_on_acl_empty, user_on_acl_empty)
    ds, source_folder = fetch(client, base)
    update_acl(client, acl_defs, ds, source_folder, source_only, default_acl, click.echo)


@update.command()
@click.argument("base", nargs=-1, required=True)
@click.option("-a", "--acl_file", "acl_file", required=True, type=click.File(), help="Path to a file containing the ACL definitions")
@click.option("-g", "--group-on-acl-empty", "group_on_acl_empty", required=False, show_default=False, default=None, help="Optional group to set ACL for if object ACLs are not present in the definition file")
@click.option("-u", "--user-on-acl-empty", "user_on_acl_empty", required=False, show_default=False, default=None, help="Optional user to set ACL for if object ACLs are not present in the definition file")
@click.pass_obj
def space_acl(args, acl_file, group_on_acl_empty, user_on_acl_empty, base):
    """
        BASE: base directory in the space hierarchy Dremio where to start applying ACLs to. Space separated. e.g. to start at a folder within a space: MYSPACE MYFOLDER
    """
    client = DremioClient(build_config(args))
    acl_defs = build_acl_defs(acl_file)
    default_acl = build_default_acl(group_on_acl_empty, user_on_acl_empty)
    ds = fetch_object_paths(client, base, len(base) if base else 0)
    update_space_acl(client, acl_defs, ds, default_acl, click.echo)


@report.command()
@click.argument("base", nargs=-1, required=True)
@click.option("-a", "--acl_file", "acl_file", required=True, type=click.File(), help="Path to a file containing the ACL definitions")
@click.option("-r", "--report-path", "report_path", required=True, type=click.Path(), help="Path where the file containing a list of all PDSs with incorrect ACLs will be written")
@click.option("-g", "--group-on-acl-empty", "group_on_acl_empty", required=False, show_default=False, default=None, help="Optional group to set ACL for if a PDS ACL list is empty")
@click.option("-u", "--user-on-acl-empty", "user_on_acl_empty", required=False, show_default=False, default=None, help="Optional user to set ACL for if a PDS ACL list is empty")
@click.pass_obj
def acl(args, acl_file, report_path, group_on_acl_empty, user_on_acl_empty, base):
    """
        BASE: base directory in Dremio where to start comparing ACLs. Space separated. e.g. to start at a db\schema within a source: MYSOURCE MYDB
    """
    client = DremioClient(build_config(args))
    acl_defs = build_acl_defs(acl_file)
    default_acl = build_default_acl(group_on_acl_empty, user_on_acl_empty)
    ds, source = fetch(client, base)
    report_acl(client, base, acl_defs, ds, report_path, default_acl, click.echo)


@dump.command()
@click.argument("base", nargs=-1, required=True)
@click.option("-d", "--dump-path", "dump_path", required=True, type=click.Path(), help="Path where the file containing a list of all ACLs will be written")
@click.pass_obj
def acl(args, dump_path, base):
    """
        BASE: base directory in Dremio where to start listing ACLs from. Space separated. e.g. to start at a db within a source: MYSOURCE MYDB
    """
    client = DremioClient(build_config(args))
    ds, source = fetch(client, base)
    dump_acl(client, ds, dump_path, base, click.echo)


@dump.command()
@click.argument("base", nargs=-1, required=False)
@click.option("-d", "--dump-path", "dump_path", required=True, type=click.Path(), help="Path where the file containing a list of all ACLs will be written")
@click.option("-v", "--include-vds", "include_vds", is_flag=True, default=False, show_default=False, required=False, help="Flag to include VDS ACLs in the dump")
@click.pass_obj
def space_acl(args, dump_path, base, include_vds):
    """
        BASE: optional base directory in Dremio where to start listing ACLs from. Space separated. e.g. to start at a folder within a space: MYSPACE MYFOLDER
    """
    client = DremioClient(build_config(args))
    objects = fetch_object_paths(client, base, len(base) if base else 0)
    dump_space_acl(client, objects, dump_path, base, click.echo, include_vds)


if __name__ == "__main__":
    sys.exit(cli())  # pragma: no cover

# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE

"""

"""

from os import remove
from os.path import exists

from metapack.cli.core import MetapackCliMemo as _MetapackCliMemo


class ArgumentError(Exception): pass

class MetapackCliMemo(_MetapackCliMemo):

    def __init__(self, args, downloader):
        super().__init__(args, downloader)


def db(subparsers):
    parser = subparsers.add_parser('db',
                                   help='Create and manipulate metatab data packages'
                                   )

    parser.set_defaults(run_command=run_metapackdb)

    parser.add_argument('-j', '--json', default=False, action='store_true',
                        help='Display configuration and diagnostic information ad JSON')

    subparsers = parser.add_subparsers()

    ## Information about packages

    info = subparsers.add_parser('info', help='Display information about a data package')
    info.set_defaults(sub_command=run_info_cmd)

    info.add_argument('url', help="Database or Datapackage URL")

    ## Import packages

    load = subparsers.add_parser('load', help='Import packages into a database')
    load.set_defaults(sub_command=run_import_cmd)
    load.add_argument('-C', '--clean', default=False, action='store_true',
                      help='Delete everything from the database first')
    load.add_argument('urls', nargs='*', help="Database or Datapackage URLS")

    ## Delete

    delete = subparsers.add_parser('delete', help='Delete packages from a database')
    delete.set_defaults(sub_command=run_delete_cmd)
    delete.add_argument('urls', nargs='*', help="Database or Datapackage URLS")

    ## List

    list = subparsers.add_parser('list', help='List packages and resources in the database')
    list.set_defaults(sub_command=run_list_cmd)
    list.add_argument('url', nargs='?', help="Database or Datapackage URLS With no package, list all packages. "
                                             "With packages, list resoruces in the packages")


def run_metapackdb(args):
    m = MetapackCliMemo(args)

    args.sub_command(m)


def run_import_cmd(m):
    if m.args.clean:
        if (m.database_url.dialect == 'sqlite'):
            if exists(m.database_url.path):
                remove(m.database_url.path)

    for p in m.packages:
        print(p)


def run_delete_cmd(m):
    pass


def run_list_cmd(m):
    pass


def run_info_cmd(m):
    pass

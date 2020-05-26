# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# Revised BSD License, included in this distribution as LICENSE

"""
Load a resource into an Sql database.
"""

import textwrap
from textwrap import dedent

from metapack import Downloader
from metapack.cli.core import MetapackCliMemo as _MetapackCliMemo
from metapack.cli.core import err, prt, warn
from sqlalchemy import (
    BLOB,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    Time,
    Unicode,
    create_engine
)
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.orm import create_session
from sqlalchemy.schema import CreateTable, DropTable
from tabulate import tabulate

downloader = Downloader()

# From http://stackoverflow.com/a/295466
def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters, etc.
    """
    import re
    import unicodedata
    value = str(value)
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('utf8').strip().lower()
    value = re.sub(r'[^\w\_\-\.]', '', value)
    value = re.sub(r'[\_\-\.]', '_', value)

    return value

class MetapackCliMemo(_MetapackCliMemo):

    def __init__(self, args, downloader):
        super().__init__(args, downloader)


class Database(object):

    def __init__(self, db_url, echo=False):
        self.db_url = db_url

        self.engine = create_engine(db_url)
        self.metadata = MetaData(bind=self.engine)
        self.session = create_session(bind=self.engine, autocommit=False, autoflush=True, echo=echo)

    def bulk_insert(self, table_name, rows):
        table = self.get_table(table_name)

        self.engine.execute(table.insert(), rows)


def sql(subparsers):
    parser = subparsers.add_parser('sql',

                                   description='Create SQL for a data package',
                                   )

    parser.set_defaults(run_command=run_sql)

    parser.add_argument('-s', '--structure', default=False, action='store_true',
                        help="Create structural sql statments, the DDL to create tables")

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument('-m', '--mysql', default=False, action='store_const', const='mysql',
                       dest='dialect', help="Generate mysql statements")

    group.add_argument('-p', '--postgres', default=False, action='store_const', const='postgresql',
                       dest='dialect', help="Generate Postgresql statements")

    group.add_argument('-q', '--sqlite', default=False, action='store_const', const='sqlite',
                       dest='dialect', help="Generate Sqlite statements")

    group.add_argument('-r', '--redshift', default=False, action='store_const', const='redshift',
                       dest='dialect', help="Generate Redshift statements")


    parser.add_argument('-C', '--create', default=False, action='store_true',
                        help="Generate table creation statements")

    parser.add_argument('-D', '--drop', default=False, action='store_true',
                        help="Generate table dropping statements")

    parser.add_argument('-L', '--load', default=False, action='store_true',
                        help="Generate data loading statements")

    parser.add_argument('-w', '--wget', default=False, action='store_const', dest='load_prog', const='wget',
                        help="For postgres format, use wget to load data")
    parser.add_argument('-u', '--curl', default=False, action='store_const', dest='load_prog', const='curl',
                        help="For postgres format, use cURL to load data")

    parser.add_argument('-A', '--access_key', help="For Redshift format, the access key to use for COPY credentials")
    parser.add_argument('-S', '--secret',  help="For Redshift format, the secret key to use for COPY credentials")
    parser.add_argument('-P', '--s3profile', help="For Redshift format, boto or aws profile to use for COPY credentials")

    parser.add_argument('metatabfile', nargs='*',
                        help="Path or URL to a metatab file. If not provided, defaults to 'metadata.csv' ")


dialect_map = {
    'msql': mysql.dialect(),
    'postgresql': postgresql.dialect(),
    'redshift': postgresql.dialect(),
    'sqlite': sqlite.dialect(),
}


type_map = {
    'unknown': Text,
    'int': Integer,
    'float': Float,
    'number': Float,
    'date': Date,
    'time': Time,
    'datetime': DateTime,
    'str': String,
    'bytes': BLOB,
    'unicode': Unicode
}


def expand_refs(r):
    from metapack import open_package, Resource
    from pathlib import Path


    if isinstance(r, Resource):
        yield r.doc, r
        return

    if isinstance(r, (list, tuple)): # Ought to be iterable type or somesuch
        for e in r:
            yield from expand_refs(e)
        return

    pkg = open_package(r)

    if not pkg.resources():
        # Metatab can open a lof of normal files, and try to interpret them, without errors,
        # but the files won't have any resources. SO, just assume it is a
        # file with a list of packages.
        for l in Path(r).open().readlines():
            yield from expand_refs(l.strip())


    if pkg.default_resource:
        yield from expand_refs(pkg.resource(pkg.default_resource))
    else:
        for r in pkg.resources():
            if r.resolved_url.proto == 'metapack':
                try:
                    yield pkg, r.resolved_url.resource
                except AttributeError:
                    yield from r.resolved_url.doc.resources()
            else:
                yield pkg, r

def run_sql(args):

    if not any([args.drop, args.create, args.load]):
        args.drop = True
        args.create = True
        args.load = True

    drop = []
    create = []
    load = []

    resources = list(expand_refs(list(args.metatabfile)))

    for doc, r in resources:
        drop.append(drop_sql(args, doc,r))
        create.append(create_sql(args, doc, r))
        load.append(load_sql(args, doc, r))

    if args.drop:
        print('\n'.join(drop))

    if args.create:
        print('\n'.join(create))

    if args.load:
        print('\n'.join(load))

    if not any([args.drop, args.create, args.load]):
        warn(f'No action specified; nothing to do. Use --create, --drop or --load')

def mk_table_name(r, doc):
    return slugify(r.name+'-'+doc.name)

def create_sql(args, doc, r):

    try:
        st = r.schema_term
    except AttributeError:
        return ''

    if not st:
        err(f"Resource '{r.name}' does not have a schema term")

    table_name =  mk_table_name(r, doc)

    table = Table(table_name, MetaData(bind=None))

    comment_rows = []

    for col in r.columns():
        # print(col)
        sql_type = type_map.get(col['datatype'], Text)

        table.append_column(Column(col['header'], sql_type,
                                   comment=col.get('description')))

        comment_rows.append((col['header'], sql_type.__name__, col['description']))

    dialect = dialect_map.get(args.dialect,mysql.dialect())

    comment=dedent(f"""
Table:       {table_name}
Description: {r.description}
Dataset:     {r.doc.name}
Columns:
{tabulate(comment_rows, tablefmt='simple')}""")

    return textwrap.indent(comment,'-- ')+'\n'+str(CreateTable(table).compile(dialect=dialect)).strip()+';'

def drop_sql(args, doc, r):

    try:
        st = r.schema_term
    except AttributeError:
        return '';

    if not st:
        err(f"Resource '{r.name}' does not have a schema term")

    table_name =  mk_table_name(r, doc)

    table = Table(table_name, MetaData(bind=None))

    dialect = dialect_map.get(args.dialect,mysql.dialect())

    lines =  str(DropTable(table).compile(dialect=dialect)).\
            replace('DROP TABLE', 'DROP TABLE IF EXISTS')

    out = []
    for l in lines.splitlines():
        if l.strip():
            out.append(l+';')

    return '\n'.join(out)

def get_credentials(profile):
    import boto3
    session =boto3.session.Session(profile_name=profile)

    return (session.get_credentials().access_key,
            session.get_credentials().secret_key)

def load_sql(args, doc,r):

    try:
        st = r.schema_term
    except AttributeError:
        return ''

    if not st:
        err(f"Resource '{r.name}' does not have a schema term")

    table_name =  mk_table_name(r, doc)

    if args.dialect == 'redshift':

        if args.access_key and args.secret:
            access_key, secret = args.access_key, args.secret
        elif args.s3profile:
            access_key, secret = get_credentials(args.s3profile)

        else:
            err('For redshift loading, must specify --access_key and --secret or --profile')

        if r.get_value('s3url'):
            cred = f"ACCESS_KEY_ID '{access_key}' SECRET_ACCESS_KEY '{secret}' ;"
            return f"""COPY {table_name} FROM '{r.s3url}' CSV {cred}"""

    elif args.dialect == 'postgresql':
        if args.load_prog:
            url = r.url

            return f"""COPY {table_name} FROM PROGRAM '{ args.load_prog} "{url}"' WITH CSV HEADER ENCODING 'utf8'; """

    elif args.dialect == 'sqlite':

        u = r.resolved_url.get_resource().get_target()
        return f".mode csv {table_name}\n.import  '{str(u.fspath)}' {table_name}"

    return ''

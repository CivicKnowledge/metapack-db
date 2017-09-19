
# Copyright (c) 2017 Civic Knowledge. This file is licensed under the terms of the
# MIT License, included in this distribution as LICENSE

"""

"""

def metapackdb(subparsers):

    parser = subparsers.add_parser(
        'db',
        help='Create and manipulate metatab data packages'
    )

    parser.set_defaults(run_command=run_metapackdb)


def run_metapackdb(arg):

    print("HERE", arg)


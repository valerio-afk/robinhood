import argparse
from argparse import Namespace,ArgumentParser
from rich.console import Console
from rich.table import  Table
from filesystem import get_rclone_remotes
from text_app import RobinHood
from backend import RobinHoodConfiguration, SyncMode



def rclone_remotes(_)->None:
    console = Console()
    remotes = get_rclone_remotes()

    table = Table(title="List of rclone remotes")
    table.add_column("Type",style="cyan")
    table.add_column("Drive", style="yellow")

    for r in remotes:
        if (len(r)>0):
            table.add_row(*r)

    console.print(table)

def make_configuration(args:Namespace) -> RobinHoodConfiguration:
    config = RobinHoodConfiguration()
    config.source_path = args.local

    if (hasattr(args,"remote")):
        config.destination_path = args.remote

    config.exclusion_filters = args.exclude
    config.deep_comparisons = args.deep
    config.exclude_hidden_files = args.exclude_hidden

    return config
def action_update(args:Namespace):
    config = make_configuration(args)
    config.sync_mode = SyncMode.UPDATE

    load_interactive()


def action_mirror(args:Namespace):
    config = make_configuration(args)
    config.sync_mode = SyncMode.MIRROR

    load_interactive()

def action_dedupe(args:Namespace):
    config = make_configuration(args)
    config.sync_mode = SyncMode.DEDUPE

    load_interactive()

def load_interactive() -> None:
    app = RobinHood()
    app.run()

    #
    # if(args.no_hidden): local.add_filter_callback(RemoveHiddenFileFilter())
    #
    # for pattern  in args.exclude:
    #     local.add_filter_callback(UnixPatternExpasionFilter(pattern))
    #
    # actions = compare_tree(local,remote)
    #
    # print(actions)


def add_sync_args(parser:ArgumentParser, include_remote:bool = True)->None:
    parser.add_argument("local", type=str, help="Local path")

    if include_remote:
        parser.add_argument("remote", type=str, help="Remote path")

    parser.add_argument("-d", "--deep", action="store_true",dest="deep", help="Matches file hash")
    parser.add_argument("-e", "--exclude", metavar="EXPR", dest="exclude", nargs="*", type=str, default=[],
                        help="List of patterns to exclude")

    parser.add_argument("-n", "--no-hidden", action="store_true",dest="exclude_hidden", help="Remove hidden files")




def main()->None:
    parser = ArgumentParser()
    parser.add_argument("--dev",action="store_true")
    subparsers = parser.add_subparsers(title="Commands", help="Available commands")

    parser_remotes = subparsers.add_parser("remotes",help="Get list of rclone remotes")
    parser_remotes.set_defaults(func=rclone_remotes)

    parser_update = subparsers.add_parser("update", help="Update remote folder with new content in local (only remote is changed)")
    add_sync_args(parser_update)
    parser_update.set_defaults(func=action_update)

    parser_mirror = subparsers.add_parser("mirror",
                                          help="Make remote folder as the local (only remote is changed)")


    add_sync_args(parser_mirror)
    parser_mirror.set_defaults(func=action_mirror)

    parser_dedupe = subparsers.add_parser("dedupe",
                                          help="Find for deduplicates in local folder")

    add_sync_args(parser_dedupe, include_remote=False)
    parser_dedupe.set_defaults(func=action_dedupe)


    args = parser.parse_args()

    if (hasattr(args,"func")):
        args.func(args)
    else:
        load_interactive()

if __name__ == '__main__':
    main()
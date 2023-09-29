import argparse
from argparse import Namespace,ArgumentParser
from rich.console import Console
from rich.table import  Table
from filesystem import get_rclone_remotes
from text_app import RobinHood
from backend import RobinHoodConfiguration



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

def action_update(args:Namespace):

    # local = LocalFileSystem(args.local, cached=True)
    # remote = RemoteFileSystem(args.remote, cached=True)

    config = RobinHoodConfiguration()
    config.source_path = args.local
    config.destination_path = args.remote
    config.exclusion_filters = args.exclude
    config.deep_comparisons = args.deep
    config.exclude_hidden_files = args.exclude_hidden

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


def add_sync_args(parser:ArgumentParser)->None:
    parser.add_argument("local", type=str, help="Local path")
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

    #parser_sync = subparsers.add_parser("sync", help="Sync local folder into remote folder (only remote is changed")
    #parser_sync.add_argument("local",type=str,help="Local path")

    parser_update = subparsers.add_parser("update", help="Update remote folder with new content in local (only remote is changed")
    add_sync_args(parser_update)
    parser_update.set_defaults(func=action_update)


    args = parser.parse_args()

    if (hasattr(args,"func")):
        args.func(args)
    else:
        load_interactive()

if __name__ == '__main__':
    main()
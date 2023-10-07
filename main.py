import json
from sys import stderr
from argparse import Namespace,ArgumentParser
from rich.panel import Panel
from rich.console import Console
from rich.table import  Table
from filesystem import get_rclone_remotes
from text_app import RobinHood
from backend import SyncMode
from config import RobinHoodProfile, RobinHoodConfiguration, get_config_file
from typing import Union


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

def make_configuration(args:Namespace) -> RobinHoodProfile:
    cfg = RobinHoodConfiguration()
    if args.profile is not None:
        try:
            profile = cfg.get_profile(args.profile)
            cfg.current_profile = profile

        except KeyError:
            print(f"Profile '{args.profile}' does not exists",file=stderr)
            exit(4)

    profile = cfg.current_profile


    if (args.local is not None): profile.source_path = args.local

    if (hasattr(args,"remote") and (args.remote is not None)):
        profile.destination_path = args.remote

    if (profile.exclude_hidden_files): profile.exclude_hidden_files = args.exclude_hidden
    if profile.deep_comparisons: profile.deep_comparisons = args.deep
    if args.exclude is not None: profile.exclusion_filters = args.exclude

    return profile
def action_update(args:Namespace) -> None:
    profile = make_configuration(args)
    profile.sync_mode = SyncMode.UPDATE

    load_interactive(profile)


def action_mirror(args:Namespace) -> None:
    profile = make_configuration(args)
    profile.sync_mode = SyncMode.MIRROR

    load_interactive(profile)

def action_dedupe(args:Namespace) -> None:
    profile = make_configuration(args)
    profile.sync_mode = SyncMode.DEDUPE

    load_interactive(profile)

def action_profiles(args:Namespace) -> None:
    #print(args)
    cfg = RobinHoodConfiguration()

    if args.list:
        console = Console()

        profiles = cfg.get_profiles()

        if (len(profiles)==0):
            print("No profiles found.")
            exit(1)
        else:
            for name,profile in profiles.items():
                console.print(Panel(str(profile),title=name))

    elif args.create is not None:
        name = args.create
        profile = RobinHoodProfile(
            source_path=args.local,
            destination_path=args.remote,
            deep_comparisons=args.deep,
            exclusion_filters=args.exclude,
            exclude_hidden_files=args.exclude_hidden
        )

        cfg.add_profile(name, profile)

    elif args.edit is not None:
        name = args.edit

        try:
            profile = cfg.get_profile(name)

            if args.local is not None: profile.source_path = args.local
            if args.remote is not None: profile.destination_path = args.remote
            if args.exclude is not None: profile.exclusion_filters = args.exclude
            profile.deep_comparisons = args.deep
            profile.exclude_hidden_files = args.exclude_hidden

            cfg.edit_profile(name, profile)
        except KeyError:
            print(f"Profile '{name}' does not exists.",file=stderr)
            exit(4)

    elif args.remove is not None:
        name = args.remove

        try:
            cfg.delete_profile(name)
        except KeyError:
            print(f"Profile '{name}' does not exists.",file=stderr)
            exit(4)



def load_interactive(profile:Union[RobinHoodProfile|None]=None) -> None:
    app = RobinHood(profile)
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
    parser.add_argument("local", type=str, help="Local path", nargs="?")

    if include_remote:
        parser.add_argument("remote", type=str, help="Remote path", nargs="?")

    parser.add_argument("-d", "--deep", action="store_true",dest="deep", help="Matches file hash")
    parser.add_argument("-e", "--exclude", metavar="EXPR", dest="exclude", nargs="*", type=str, default=None,
                        help="List of patterns to exclude")

    parser.add_argument("-n", "--no-hidden", action="store_true",dest="exclude_hidden", help="Remove hidden files")

def add_profile_args(parser:ArgumentParser) -> None:
    parser.add_argument("-p", "--profile",type=str,required=False,default=None,help="Load a synchronisation profile")




def main()->None:
    parser = ArgumentParser()
    parser.add_argument("--dev",action="store_true")
    subparsers = parser.add_subparsers(title="Commands", help="Available commands")

    parser_remotes = subparsers.add_parser("remotes",help="Get list of rclone remotes")
    parser_remotes.set_defaults(func=rclone_remotes)

    parser_profile = subparsers.add_parser("profile", help="Create a user profile")

    profile_group = parser_profile.add_mutually_exclusive_group()
    profile_group.add_argument("-l","--list",action='store_true',dest="list",help="List all profiles")
    profile_group.add_argument("-c", "--create", metavar="NAME", type=str, dest="create", help="Create a new profile")
    profile_group.add_argument("-m", "--edit", metavar="NAME", type=str, dest="edit", help="Edit an existing profile")
    profile_group.add_argument("-r", "--remove", metavar="NAME", type=str, dest="remove", help="Remove a profile")
    add_sync_args(parser_profile)

    parser_profile.set_defaults(func=action_profiles)

    parser_update = subparsers.add_parser("update", help="Update remote folder with new content in local (only remote is changed)")
    add_sync_args(parser_update)
    add_profile_args(parser_update)
    parser_update.set_defaults(func=action_update)

    parser_mirror = subparsers.add_parser("mirror",
                                          help="Make remote folder as the local (only remote is changed)")


    add_sync_args(parser_mirror)
    parser_mirror.set_defaults(func=action_mirror)
    add_profile_args(parser_mirror)

    parser_dedupe = subparsers.add_parser("dedupe",
                                          help="Find for deduplicates in local folder")

    add_sync_args(parser_dedupe, include_remote=False)
    add_profile_args(parser_dedupe)
    parser_dedupe.set_defaults(func=action_dedupe)


    args = parser.parse_args()

    try:
        RobinHoodConfiguration() #read/init conf file

        if (hasattr(args,"func")):
            args.func(args)
        else:
            load_interactive()
    except json.decoder.JSONDecodeError as e:
        print(f"Unable to read the configuration file in {get_config_file()}", file=stderr)
        print(e, file=stderr)
        exit(2)

if __name__ == '__main__':
    main()
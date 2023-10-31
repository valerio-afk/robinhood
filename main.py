# Copyright (c) 2023 Valerio AFK <afk.broadcast@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import json
import asyncio
from sys import stderr
from argparse import Namespace, ArgumentParser
from rich.panel import Panel
from rich.console import Console
from rich.table import Table
from text_app import RobinHood
from backend import SyncMode
from config import RobinHoodProfile, RobinHoodConfiguration, get_config_file
from typing import Union, Callable

import sys
sys.path.append("/home/tuttoweb/Documents/repositories/pyrclone")
from pyrclone import rclone


def run_in_asyncio(func:Callable)->Callable:

    def runner(*args,**kwargs):
        asyncio.run(func(*args,**kwargs))

    return runner
@run_in_asyncio
async def rclone_remotes(args:Namespace) -> None:
    '''
    Print on screen the list of remote directories configured on rclone
    '''

    # create a new console interface (rich)
    console = Console()

    # make a new table (with rich) with two columns
    table = Table(title="List of rclone remotes")
    table.add_column("Type", style="cyan")
    table.add_column("Drive", style="yellow")

    # for each of the remote directories obtained from the
    # function above, a row is added to the table
    async with rclone() as rc:
        for r in await rc.list_remotes():
            if len(r) > 0:
                table.add_row(*r)

    # print the formatted table to the console
    console.print(table)


def make_configuration(args: Namespace) -> RobinHoodProfile:
    '''
    This function returns a suitable user profile (which can be new or loaded as specified by the user).
    Profiles can be overridden over the command line using suitable flags

    :param args: Command-line arguments
    :return: An object of type RobinHoodProfile containing the configuration of a new or chosen profile
    '''

    # Read the current configuration file
    cfg = RobinHoodConfiguration()

    # Check if the user has provided a profile name to load
    if args.profile is not None:
        try:
            # If the user has provided a profile name, it will be loaded
            # if it exists
            profile = cfg.get_profile(args.profile)

            # Set the wanted profile as the current profile
            cfg.current_profile = profile

        except KeyError:
            # In case the provided profile doesn't exist, the method .get_profile will raise
            # a KeyError exception that it'll be caught and managed here by printing
            # an error message to the standard error
            print(f"Profile '{args.profile}' does not exists", file=stderr)
            exit(4)

    # Whether a profile is specified or not, the current profile is retrieved by the configuration,
    # which can even be a brandnew (empty) profile
    profile = cfg.current_profile

    # At this point, the user can overwrite the profile over the command line
    # Let's check if the local directory path needs to be overridden
    if args.local is not None:
        profile.source_path = args.local

    # Let's check if the remote directory path needs to be overridden
    # In some cases (e.g., dedupe command), a remote argument may not even exist.
    # For this reason, it's required to check if `remote` is part of the args Namespace
    if (hasattr(args, "remote") and (args.remote is not None)):
        profile.destination_path = args.remote


    # Let's check if the user wants to exclude hidden files
    if (args.exclude_hidden):
        profile.exclude_hidden_files = args.exclude_hidden

    # Let's check if the user wants to perform deep comparison
    if args.deep:
        profile.deep_comparisons = args.deep

    # Let's check if the user wants to override the exclusion filters
    if args.exclude is not None:
        profile.exclusion_filters = args.exclude

    if (args.on_completion is not None):
        profile.on_completion = args.on_completion

    if args.clear_cache:
        profile.clear_cache()

    return profile


def action_update(args: Namespace) -> None:
    '''
    Performs the update between local and remote (no changes in local)

    :param args: Command-line arguments
    '''
    profile = make_configuration(args)
    profile.sync_mode = SyncMode.UPDATE

    #TODO: all the batch work needs to be coded here

    open_tui(profile)


def action_mirror(args: Namespace) -> None:
    '''
    Performs mirror synchronisation between local and remote (no changes in local)

    :param args: Command-line arguments
    '''

    profile = make_configuration(args)
    profile.sync_mode = SyncMode.MIRROR

    # TODO: all the batch work needs to be coded here

    open_tui(profile)

def action_sync(args: Namespace) -> None:
    '''
    Performs mirror synchronisation between local and remote (no changes in local)

    :param args: Command-line arguments
    '''

    profile = make_configuration(args)
    profile.sync_mode = SyncMode.SYNC

    # TODO: all the batch work needs to be coded here

    open_tui(profile)


def action_dedupe(args: Namespace) -> None:
    '''
    Performs deduplicate checks in the provided directory (can be either local or remote)
    :param args: Command-line arguments
    '''
    profile = make_configuration(args)
    profile.sync_mode = SyncMode.DEDUPE

    # TODO: all the batch work needs to be coded here

    open_tui(profile)


def action_profiles(args: Namespace) -> None:
    '''
    Command line management of the profiles
    :param args: Command-line arguments
    '''

    # Read the configuration file
    cfg = RobinHoodConfiguration()

    #Let's check what the user wants to do
    #Do they want to list all the profiles?
    if args.list:
        # Make a new console interface
        console = Console()

        # Get all the current profiles from the configuration files
        profiles = cfg.get_profiles()

        #Are there any profiles?
        if (len(profiles) == 0):
            # If not, an error message is printed in the standard error
            print("No profiles found.",file=stderr)
            exit(1)
        else:
            # If there are profiles, a loop will print them all in the console
            for name, profile in profiles.items():
                console.print(Panel(str(profile), title=name))
    # Do they want to create a new profile?
    elif args.create is not None:
        # Get the name from the command line
        name = args.create

        # A new RobinHoodProfile object is created getting the information from the command line
        profile = RobinHoodProfile(
            source_path=args.local,
            destination_path=args.remote,
            deep_comparisons=args.deep,
            exclusion_filters=args.exclude,
            exclude_hidden_files=args.exclude_hidden,
            on_completion=args.on_completion if args.on_completion is None else "NOTHING"
        )

        # The new profile is added to the configuration file
        cfg.add_profile(name, profile)

    # Do they want to edit an existing profile?
    elif args.edit is not None:
        # Get the name from the command line
        name = args.edit

        try:
            # Retrieve the selected profile (if exists)
            profile = cfg.get_profile(name)

            # Change all the specified parameters

            if args.local is not None: profile.source_path = args.local
            if args.remote is not None: profile.destination_path = args.remote
            if args.exclude is not None: profile.exclusion_filters = args.exclude
            if args.on_completion is not None: profile.on_completion = args.on_completion

            if (args.deep):
                profile.deep_comparisons = True
            elif args.no_deep:
                profile.deep_comparisons = False

            if args.exclude_hidden:
                profile.exclude_hidden_files = True
            elif args.include_hidden:
                profile.exclude_hidden_files = False

            # Perform the change of the profile on the configuration file
            cfg.edit_profile(name, profile)
        except KeyError:
            # The selected profile does not exist and an error is shown in the standard error
            print(f"Profile '{name}' does not exists.", file=stderr)
            exit(4)

    elif args.remove is not None:
        # Do they want to edit an existing profile?
        name = args.remove

        try:
            # Attempt to perform the deletion of the selected profile
            cfg.delete_profile(name)
        except KeyError:
            # The selected profile does not exist and an error is shown in the standard error
            print(f"Profile '{name}' does not exists.", file=stderr)
            exit(4)


def open_tui(profile: Union[RobinHoodProfile | None] = None) -> None:
    '''
    Opens the Textual User Interface (Interactive)
    :param profile: A RobinHoodProfile with pre-filled configuration (or None)
    '''

    app = RobinHood(profile)
    app.run()


def add_sync_args(parser: ArgumentParser, include_remote: bool = True, extend_binary_flags=False) -> None:
    '''
    Adds some standard command line arguments related to synching
    :param parser: The parser where these arguments need to be added
    :param include_remote: Whether the 'remote' positional argument needs to be added (useful for dedupe)
    :param extend_binary_flags: Adds other binary flags in opposition to others (e.g., add --hidden against --no-hidden)
                                Useful to create/edit a profile
    '''

    parser.add_argument("local",
                        type=str,
                        help="Local path",
                        nargs="?")

    if include_remote:
        parser.add_argument("remote",
                            type=str,
                            help="Remote path",
                            nargs="?")

    parser.add_argument("-e",
                        "--exclude",
                        metavar="EXPR",
                        dest="exclude",
                        nargs="*",
                        type=str,
                        default=None,
                        help="List of patterns to exclude")

    parser.add_argument("--clear-cache",action="store_true",dest="clear_cache",help="Clear file tree structure cache")

    deep_search_group = parser.add_mutually_exclusive_group()
    hidden_files_group = parser.add_mutually_exclusive_group()

    deep_search_group.add_argument("-d",
                                   "--deep",
                                   action="store_true",
                                   dest="deep",
                                   help="Compare also file content")

    hidden_files_group.add_argument("-n",
                                    "--no-hidden",
                                    action="store_true",
                                    dest="exclude_hidden",
                                    help="Exclude hidden files")

    if extend_binary_flags:
        deep_search_group.add_argument("--no-deep",
                                       action="store_true",
                                       dest="no_deep",
                                       help="Doesn't compare file content")
        hidden_files_group.add_argument("--hidden",
                                        action="store_true",
                                        dest="include_hidden",
                                        help="Include hidden files")


    on_completion_group = parser.add_mutually_exclusive_group()
    on_completion_group.add_argument("--on-completion",
                                     metavar="COMMAND",
                                     dest="on_completion",
                                     default=None,
                                     help="Run a command on completion")

    on_completion_group.add_argument("--shutdown",
                                     dest="on_completion",
                                     action="store_const",
                                     const="SHUTDOWN",
                                     help="Shuts down the computer on completion")
    
    on_completion_group.add_argument("--suspend",
                                     dest="on_completion",
                                     action="store_const",
                                     const="SUSPEND",
                                     help="Suspends the computer on completion")


def add_profile_args(parser: ArgumentParser) -> None:
    '''
    Adds a command line argument to get the name of a profile
    :param parser:
    :return:
    '''
    parser.add_argument("-p", "--profile", type=str, required=False, default=None,
                        help="Load a synchronisation profile")


def main() -> None:
    '''
    Program's entry point
    '''

    # Create a new command line argument parser
    parser = ArgumentParser()

    # Subparser container
    subparsers = parser.add_subparsers(title="Commands", help="Available commands")

    # REMOTE command
    parser_remotes = subparsers.add_parser("remotes", help="Get list of rclone remotes")
    parser_remotes.set_defaults(func=rclone_remotes)

    # PRIFILE command
    parser_profile = subparsers.add_parser("profile", help="Create a user profile")

    profile_group = parser_profile.add_mutually_exclusive_group()
    profile_group.add_argument("-l", "--list", action='store_true', dest="list", help="List all profiles")
    profile_group.add_argument("-c", "--create", metavar="NAME", type=str, dest="create", help="Create a new profile")
    profile_group.add_argument("-m", "--edit", metavar="NAME", type=str, dest="edit", help="Edit an existing profile")
    profile_group.add_argument("-r", "--remove", metavar="NAME", type=str, dest="remove", help="Remove a profile")
    add_sync_args(parser_profile, extend_binary_flags=True)

    parser_profile.set_defaults(func=action_profiles)

    # UPDATE command
    parser_update = subparsers.add_parser("update",
                                          help="Update remote folder with new content in local (only remote is changed)")

    add_sync_args(parser_update)
    add_profile_args(parser_update)

    parser_update.set_defaults(func=action_update)

    # MIRROR command
    parser_mirror = subparsers.add_parser("mirror", help="Make remote folder as the local (only remote is changed)")

    add_sync_args(parser_mirror)
    add_profile_args(parser_mirror)
    parser_mirror.set_defaults(func=action_mirror)

    # SYNC command
    parser_sync = subparsers.add_parser("sync", help="Make remote folder as the local (only remote is changed)")

    add_sync_args(parser_sync)
    add_profile_args(parser_sync)
    parser_sync.set_defaults(func=action_sync)

    # DEDUPE command

    parser_dedupe = subparsers.add_parser("dedupe", help="Find for deduplicates in local folder")

    add_sync_args(parser_dedupe, include_remote=False)
    add_profile_args(parser_dedupe)
    parser_dedupe.set_defaults(func=action_dedupe)

    # parse arguments from command line
    args = parser.parse_args()

    # read configuration file
    try:
        RobinHoodConfiguration()  # read/init conf file

        # check if a command is specified
        if (hasattr(args, "func")):
            args.func(args)
        else:
            # if no command is specified, launches the TUI
            open_tui()
    except json.decoder.JSONDecodeError as e:
        # this occurs if the JSON is malformed
        print(f"Unable to read the configuration file in {get_config_file()}", file=stderr)
        print(e, file=stderr)
        exit(2)


if __name__ == '__main__':
    main()

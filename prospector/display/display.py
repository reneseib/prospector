import os
import sys


class style:
    PURPLE = "\033[95m"
    CYAN = "\033[96m"
    DARKCYAN = "\033[36m"
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    END = "\033[0m"

    LINE_UP = "\033[1A"
    LINE_CLEAR = "\x1b[2K"


class Display:
    @staticmethod
    def start_screen():
        if sys.stdout.isatty():
            terminal_size = os.get_terminal_size()
            terminal_width = terminal_size.columns
            if terminal_width % 2 != 0:
                terminal_width = terminal_width - 1

            side = int(int(terminal_width - len("PROSPECTOR") - 5) / 2)

            print(style.YELLOW + style.BOLD + "-" * (terminal_width) + style.END)
            print(
                style.BOLD
                + style.CYAN
                + ("-" * side)
                + style.YELLOW
                + " ☼ PROSPECTOR ☼ "
                + style.CYAN
                + ("-" * side)
                + style.END
            )
            print(style.YELLOW + style.BOLD + "-" * (terminal_width) + style.END)
            print("")
            return None
        else:

            print(
                style.BOLD
                + style.CYAN
                + ("-" * 10)
                + style.YELLOW
                + " ☼ PROSPECTOR ☼ "
                + style.CYAN
                + ("-" * 10)
                + style.END
            )
            print("")
            return None

    @staticmethod
    def dir_tree_warning():
        print(
            style.BOLD
            + style.PURPLE
            + "Mandatory directory tree does not yet exist."
            + style.END
        )
        print("Prospector will build the directory tree now.")
        return None

    @staticmethod
    def dir_tree_build_success():
        print(
            style.BOLD
            + style.GREEN
            + "Successfully built the directory tree"
            + style.END
        )
        return None

    @staticmethod
    def import_error():
        print("\n" + style.BOLD, style.RED)
        print("Error: No pconfig.py found." + style.END)
        print("Make sure you set up the pconfig.py in your project directory first.")
        print("\n")
        return None

    # Stage Displays
    @staticmethod
    def stage_done(stage_formal):
        print(style.LINE_UP, end=style.LINE_CLEAR)
        print(style.BOLD + style.PURPLE + f"{stage_formal}:\tAlready done" + style.END)
        return None

    @staticmethod
    def stage_finished(stage_formal):
        print(style.BOLD + style.PURPLE + f"{stage_formal}: Finished" + style.END)
        return None

    @staticmethod
    def stage_proc_error(stage_formal):
        print(
            style.BOLD
            + style.RED
            + f"{stage_formal}: Some error occured during processing."
            + style.END
        )
        return None

    @staticmethod
    def status_bar(current, max_len):
        if sys.stdout.isatty():
            fullwidth = int(os.get_terminal_size().columns)
            width = int(fullwidth - 25)
        else:
            fullwidth = 75
            width = 68
        proc = int(round((i / len(gdf)) * width))
        rest = int(width - proc)
        # print("\r" * 7)
        # print("=" * os.get_terminal_size().columns)
        print(" " * fullwidth, end="\r")
        print(
            f">> Working: |"
            + "▉" * proc
            + f"{'-'*rest}| {round((i / len(gdf)) * 100, 2):03.2f} %",
            end="\r",
        )
        print(" " * fullwidth, end="\r")
        return None

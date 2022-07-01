import os


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

    def dir_tree_warning():
        print(
            style.BOLD
            + style.PURPLE
            + "Mandatory directory tree does not yet exist."
            + style.END
        )
        print("Prospector will build the directory tree now.")
        return None

    def dir_tree_build_success():
        print(
            style.BOLD
            + style.GREEN
            + "Successfully built the directory tree"
            + style.END
        )
        return None

    def import_error():
        print("\n" + style.BOLD, style.RED)
        print("Error: No pconfig.py found." + style.END)
        print("Make sure you set up the pconfig.py in your project directory first.")
        print("\n")
        return None

    # Stage Displays
    def stage_done(stage_formal):
        print(style.LINE_UP, end=style.LINE_CLEAR)
        print(style.BOLD + style.PURPLE + f"{stage_formal}:\tAlready done" + style.END)
        return None

    def stage_finished(stage_formal):
        print(style.BOLD + style.PURPLE + f"{stage_formal}: Finished" + style.END)
        return None

    def stage_proc_error(stage_formal):
        print(
            style.BOLD
            + style.RED
            + f"{stage_formal}: Some error occured during processing."
            + style.END
        )
        return None


from typing import Any

# Add Some Color ==========================


class bcolors:
    """Magic sequences that make the Terminal o utput COLORFUL."""
    """ ANSI color codes """

    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'

    ENDC = '\033[0m'
    BOLD = '\033[1m'
    BLUE = BLUE = "\033[0;34m"
    UNDERLINE = '\033[4m'
    BLACK = "\033[0;30m"
    RED = "\033[0;31m"
    GREEN = "\033[0;32m"
    BROWN = "\033[0;33m"
    BLUE = "\033[0;34m"
    PURPLE = "\033[0;35m"
    CYAN = "\033[0;36m"
    YELLOW = "\033[1;33m"

    BRIGHT_BLACK = '\033[90m'
    BRIGHT_RED = '\033[91m'
    BRIGHT_GREEN = '\033[92m'
    BRIGHT_YELLOW = '\033[93m'
    BRIGHT_BLUE = '\033[94m'
    BRIGHT_MAGENTA = '\033[95m'
    BRIGHT_CYAN = '\033[96m'
    BRIGHT_WHITE = '\033[97m'

    LIGHT_GRAY = "\033[0;37m"
    DARK_GRAY = "\033[1;30m"
    LIGHT_RED = "\033[1;31m"
    LIGHT_GREEN = "\033[1;32m"
    LIGHT_BLUE = "\033[1;34m"
    LIGHT_PURPLE = "\033[1;35m"
    LIGHT_CYAN = "\033[1;36m"
    LIGHT_WHITE = "\033[1;37m"
    FAINT = "\033[2m"
    ITALIC = "\033[3m"
    BLINK = "\033[5m"
    NEGATIVE = "\033[7m"
    CROSSED = "\033[9m"
    ORANGE = "\033[38;5;208m"


def _bcolor_it(s, bc, ):
    """Wraps a string in bcolors."""
    return f'{bc}{s}{bcolors.ENDC}'


def _bold(s: Any):
    """Helper routing for bold()"""
    return _bcolor_it(str(s), bcolors.BRIGHT_YELLOW, )


def bold(*s: Any):
    """Prints a string in BOLD."""
    print(_bcolor_it(" ".join(str(arg) for arg in s), bcolors.BRIGHT_YELLOW, ))


def _red(s: Any):
    """Helper routine for RED."""
    return _bcolor_it(str(s), bcolors.RED, )


def red(*s: Any):
    """Prints string in RED."""
    print(_bcolor_it(" ".join(str(arg) for arg in s), bcolors.RED, ))


def _lightRed(s: Any):
    return (_bcolor_it(str(s), bcolors.LIGHT_RED,))


def lightRed(*s: Any):
    print(_bcolor_it(" ".join(str(arg) for arg in s), bcolors.LIGHT_RED,))


def _green(s: Any):
    """Helper routine for GREEN."""
    return _bcolor_it(str(s), bcolors.BRIGHT_GREEN, )


def green(*s: Any):
    """Prints string in GREEN."""
    print(_bcolor_it(" ".join(str(arg) for arg in s), bcolors.BRIGHT_GREEN, ))


def _lightGreen(s: Any):
    return _bcolor_it(str(s), bcolors.LIGHT_GREEN, )


def _blue(s: Any):
    return _bcolor_it(str(s), bcolors.BLUE, )


def blue(*s: Any):
    print(_bcolor_it(" ".join(str(arg) for arg in s), bcolors.BLUE, ))


def lightGreen(*s: Any):
    print(_bcolor_it(" ".join(str(arg) for arg in s), bcolors.LIGHT_GREEN, ))


def _lightBlue(s: Any):
    return _bcolor_it(str(s), bcolors.LIGHT_BLUE, )


def lightBlue(*s: Any):
    print(_bcolor_it(" ".join(str(arg) for arg in s), bcolors.LIGHT_BLUE, ))


def _orange(s: Any):
    return _bcolor_it(str(s), bcolors.ORANGE, )


def orange(*s: Any):
    print(_bcolor_it(" ".join(str(arg) for arg in s), bcolors.ORANGE, ))


def _underline(s: Any):
    return (_bcolor_it(str(s), bcolors.UNDERLINE,))


def underline(*s: Any):
    print(_bcolor_it(" ".join(str(arg) for arg in s), bcolors.UNDERLINE,))


def _yellow(s: Any):
    return (_bcolor_it(str(s), bcolors.YELLOW,))


def yellow(*s: Any):
    print(_bcolor_it(" ".join(str(arg) for arg in s), bcolors.YELLOW,))


def _lightWhite(s: Any):
    return (_bcolor_it(str(s), bcolors.LIGHT_WHITE,))


def lightWhite(*s: Any):
    print(_bcolor_it(" ".join(str(arg) for arg in s), bcolors.LIGHT_WHITE,))


def _lightPurple(s: Any):
    return (_bcolor_it(str(s), bcolors.LIGHT_PURPLE,))


def lightPurple(*s: Any):
    print(_bcolor_it(" ".join(str(arg) for arg in s), bcolors.LIGHT_PURPLE,))


def _lightGray(s: Any):
    return (_bcolor_it(str(s), bcolors.LIGHT_GRAY,))


def lightGray(*s: Any):
    print(_bcolor_it(" ".join(str(arg) for arg in s), bcolors.LIGHT_GRAY,))


def _black(s: Any):
    return (_bcolor_it(str(s), bcolors.BLACK,))


def black(*s: Any):
    print(_bcolor_it(" ".join(str(arg) for arg in s), bcolors.BLACK,))


def _cyan(s: Any):
    return (_bcolor_it(str(s), bcolors.CYAN,))


def cyan(*s: Any):
    print(_bcolor_it(" ".join(str(arg) for arg in s), bcolors.CYAN,))


def _lightCyan(s: Any):
    return (_bcolor_it(str(s), bcolors.LIGHT_CYAN,))


def lightCyan(*s: Any):
    print(_bcolor_it(" ".join(str(arg) for arg in s), bcolors.LIGHT_CYAN,))

# End add Some Color ==========================


if __name__ == '__main__':
    print("Testing all color routines:\n")

    # Basic colors
    print("=== Basic Colors ===")

    bold('1', '2', '333')

    red('RED', 'text', 'with', 'multiple', 'args')
    green('GREEN', 'text')
    blue('BLUE', 'text')
    yellow('YELLOW', 'text')
    cyan('CYAN', 'text')
    black('BLACK', 'text')

    # Light colors
    print("\n=== Light Colors ===")
    lightGreen('LIGHT', 'GREEN', 'text')
    lightBlue('LIGHT', 'BLUE', 'text')
    light_white('LIGHT', 'WHITE', 'text')
    light_purple('LIGHT', 'PURPLE', 'text')
    light_gray('LIGHT', 'GRAY', 'text')
    light_cyan('LIGHT', 'CYAN', 'text')
    light_red('LIGHT', 'RED', 'text')

    # Special formatting
    print("\n=== Special Formatting ===")
    bold('BOLD', 'text', 'with', 'args')
    underline('UNDERLINED', 'text')
    orange('ORANGE', 'text')

    # Test helper functions (return strings without printing)
    print("\n=== Testing Helper Functions (non-printing) ===")
    print("Helper function outputs:")
    print(f"_red: {_red('red helper')}")
    print(f"_green: {_green('green helper')}")
    print(f"_blue: {_blue('blue helper')}")
    print(f"_lightBlue: {_lightBlue('light blue helper')}")
    print(f"_lightGreen: {_lightGreen('light green helper')}")
    print(f"_orange: {_orange('orange helper')}")
    print(f"_yellow: {_yellow('yellow helper')}")
    print(f"_light_white: {_light_white('light white helper')}")
    print(f"_light_purple: {_light_purple('light purple helper')}")
    print(f"_light_gray: {_light_gray('light gray helper')}")
    print(f"_black: {_black('black helper')}")
    print(f"_cyan: {_cyan('cyan helper')}")
    print(f"_light_cyan: {_light_cyan('light cyan helper')}")
    print(f"_light_red: {_light_red('light red helper')}")
    print(f"_bold: {_bold('bold helper')}")
    print(f"_underline: {_underline('underline helper')}")

    print("\n=== Color Demonstration Complete ===")
    print("All color routines have been tested!")

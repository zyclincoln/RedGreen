# encoding: utf-8
import logging


class ColoredFormatter(logging.Formatter):
    white = "\x1b[1m"
    blue = "\x1b[1;36;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: white + format + reset,
        logging.INFO: blue + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def setup_logger():
    logger = logging.getLogger('REDGREEN')
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(ColoredFormatter())
        logger.addHandler(console_handler)

    return logger


# Export the configured logger
logger = setup_logger()


if __name__ == "__main__":
    logger.debug("DEBUG TEST")
    logger.info("INFO TEST")
    logger.warning("WARN TEST")
    logger.error("ERROR TEST")
    logger.critical("CRITICAL TEST")

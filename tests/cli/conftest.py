# Compatibility shim for Click 8.x which removed the mix_stderr parameter from CliRunner
import click.testing

_original_init = click.testing.CliRunner.__init__


def _patched_init(self, *args, mix_stderr=True, **kwargs):
    _original_init(self, *args, **kwargs)


click.testing.CliRunner.__init__ = _patched_init

import os

import pytest

from feedkit.cli import main


@pytest.mark.skipif("DATABASE_URL" not in os.environ, reason="needs Postgres")
def test_init_db_command(capsys):
    assert main(["init-db"]) == 0
    assert "schema applied" in capsys.readouterr().out


def test_unknown_source_is_rejected():
    with pytest.raises(SystemExit):
        main(["run", "--source", "nope"])

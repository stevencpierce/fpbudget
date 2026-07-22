"""Alembic environment (audit H5, 2026-07-20).

Go-forward migrations ONLY: the pre-Alembic schema was built by app.py's
boot-time DDL, which stays frozen for the columns it already manages. Every
NEW schema change from now on is a hand-written revision in alembic/versions/
— no autogenerate (models vs. live-DB drift would produce noise), so
target_metadata is intentionally None.

URL resolution mirrors app.py: DATABASE_URL env var, with Render's legacy
postgres:// scheme rewritten for SQLAlchemy 2.
"""
import os
from sqlalchemy import engine_from_config, pool
from alembic import context

config = context.config

_url = os.getenv("DATABASE_URL", "")
if _url:
    config.set_main_option(
        "sqlalchemy.url", _url.replace("postgres://", "postgresql://", 1))

target_metadata = None


def run_migrations_offline():
    context.configure(url=config.get_main_option("sqlalchemy.url"),
                      literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

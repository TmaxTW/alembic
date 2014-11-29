from alembic.testing.requirements import SuiteRequirements
from alembic.testing import exclusions


class DefaultRequirements(SuiteRequirements):

    @property
    def schemas(self):
        """Target database must support external schemas, and have one
        named 'test_schema'."""

        return exclusions.skip_if([
            "sqlite",
            "firebird"
        ], "no schema support")

    @property
    def no_referential_integrity(self):
        """test will fail if referential integrity is enforced"""

        return exclusions.fails_on_everything_except("sqlite")

    @property
    def non_native_boolean(self):
        """test will fail if native boolean is provided"""

        return exclusions.fails_if(
            exclusions.LambdaPredicate(
                lambda config: config.db.dialect.supports_native_boolean
            )
        )

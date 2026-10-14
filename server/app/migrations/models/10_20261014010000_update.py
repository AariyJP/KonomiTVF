from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        ALTER TABLE "users" ADD COLUMN "discord_user_id" TEXT;
        ALTER TABLE "users" ADD COLUMN "discord_user_name" TEXT;
        ALTER TABLE "users" ADD COLUMN "discord_user_icon_url" TEXT;
        ALTER TABLE "users" ADD COLUMN "discord_access_token" TEXT;
        ALTER TABLE "users" ADD COLUMN "discord_refresh_token" TEXT;
    """


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        ALTER TABLE "users" DROP COLUMN "discord_refresh_token";
        ALTER TABLE "users" DROP COLUMN "discord_access_token";
        ALTER TABLE "users" DROP COLUMN "discord_user_icon_url";
        ALTER TABLE "users" DROP COLUMN "discord_user_name";
        ALTER TABLE "users" DROP COLUMN "discord_user_id";
    """

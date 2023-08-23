-- macros/drop_and_create_table.sql

{% macro drop_and_create_table(table_name) %}
    {% set schema_name, table_name = table_name.split('.') %}
    DROP TABLE IF EXISTS "{{ schema_name }}"."{{ table_name }}";

    CREATE TABLE "public"."profileInfo" AS
    SELECT
        u.userId as userId,
        u.profileList[0]."id" as profileId,
        u.profileList[0]."name" as name,
        u.profileList[0]."email" as email,
        u.profileList[0]."phone" as phone
    FROM basic_profile.userprofile AS u;
{% endmacro %}

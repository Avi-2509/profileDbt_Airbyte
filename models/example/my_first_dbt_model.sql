
  create  table
    "dev".basic_profile."userprofile__dbt_tmp"


      compound sortkey(_airbyte_emitted_at)

  as (

with __dbt__cte__userprofile_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "dev".basic_profile._airbyte_raw_userprofile
select
    case when _airbyte_data."createdAt" != '' then _airbyte_data."createdAt" end as createdat,
    _airbyte_data."profileList" as profilelist,
    case when _airbyte_data."id" != '' then _airbyte_data."id" end as id,
    case when _airbyte_data."_id" != '' then _airbyte_data."_id" end as _id,
    case when _airbyte_data."_class" != '' then _airbyte_data."_class" end as _class,
    case when _airbyte_data."userId" != '' then _airbyte_data."userId" end as userid,
    case when _airbyte_data."status" != '' then _airbyte_data."status" end as status,
    case when _airbyte_data."updatedAt" != '' then _airbyte_data."updatedAt" end as updatedat,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    getdate() as _airbyte_normalized_at
from "dev".basic_profile._airbyte_raw_userprofile as table_alias
-- userprofile
where 1 = 1
),  __dbt__cte__userprofile_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__userprofile_ab1
select
    cast(createdat as text) as createdat,
    profilelist,
    cast(id as text) as id,
    cast(_id as text) as _id,
    cast(_class as text) as _class,
    cast(userid as text) as userid,
    cast(status as
    float
) as status,
    cast(updatedat as text) as updatedat,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    getdate() as _airbyte_normalized_at
from __dbt__cte__userprofile_ab1
-- userprofile
where 1 = 1
),  __dbt__cte__userprofile_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__userprofile_ab2
select
    md5(cast(coalesce(cast(createdat as text), '') || '-' || coalesce(cast(json_serialize(profilelist) as text), '') || '-' || coalesce(cast(id as text), '') || '-' || coalesce(cast(_i
d as text), '') || '-' || coalesce(cast(_class as text), '') || '-' || coalesce(cast(userid as text), '') || '-' || coalesce(cast(status as text), '') || '-' || coalesce(cast(updatedat
 as text), '') as text)) as _airbyte_userprofile_hashid,
    tmp.*
from __dbt__cte__userprofile_ab2 tmp
-- userprofile
where 1 = 1
)-- Final base SQL model
-- depends_on: __dbt__cte__userprofile_ab3
select
    createdat,
    profilelist,
    id,
    _id,
    _class,
    userid,
    status,
    updatedat,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    getdate() as _airbyte_normalized_at,
    _airbyte_userprofile_hashid
from __dbt__cte__userprofile_ab3
-- userprofile from "dev".basic_profile._airbyte_raw_userprofile
where 1 = 1
  );bash-4.2#

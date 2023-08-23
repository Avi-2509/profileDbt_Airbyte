
{{ config(materialized='table') }}
select
    u.userId as userId,
    u.profileList[0]."id" as profileId,
    u.profileList[0]."name" as name,
    u.profileList[0]."email" as email,
    u.profileList[0]."phone" as phone
from basic_profile.userprofile as u
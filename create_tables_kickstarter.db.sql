CREATE TABLE "urls_to_scrape" (
	`url`	TEXT NOT NULL,
	PRIMARY KEY(url)
);
CREATE TABLE all_files
(
  project_id int          not null
    primary key,
  url        varchar(190) null
);
CREATE TABLE category
(
  id                int          not null
    primary key,
  name              varchar(190) null,
  parent_id         int          null,
  position          int          null,
  color             int          null,
  slug              varchar(190) null,
  urls_web_discover text         null
);
CREATE TABLE creator
(
  id            int          not null
    primary key,
  name          varchar(190) null,
  slug          varchar(190) null,
  avatar_thumb  text         null,
  avatar_small  text         null,
  urls_web_user text         null,
  avatar_medium text         null,
  is_registered varchar(20)  null,
  urls_api_user text         null
, chosen_currency TEXT NULL);
CREATE TABLE funding_trend
(
  projectid                 int                                 not null,
  project_last_modification timestamp default CURRENT_TIMESTAMP not null,
  amount_pledged_usd        decimal(20, 10)                     null,
  backer_count              int                                 null,
  update_count              int                                 null,
  comment_count             int                                 null,
  status                    varchar(20)                         not null,
  primary key (projectid, project_last_modification)
);
CREATE TABLE item
(
  id         int         not null,
  project_id int         not null,
  taxable    varchar(20) null,
  name       text        null,
  edit_path  text        null,
  amount     int         null,
  primary key (project_id, id)
);
CREATE TABLE livestream
(
  id         int         not null
    primary key,
  project_id int         null,
  live_now   varchar(20) null,
  name       text        null,
  start_date int         null,
  url        text        null
);
CREATE TABLE location
(
  id                       int          not null
    primary key,
  displayable_name         varchar(190) null,
  type                     varchar(190) null,
  name                     varchar(190) null,
  state                    varchar(190) null,
  short_name               varchar(190) null,
  is_root                  varchar(20)  null,
  country                  varchar(20)  null,
  slug                     varchar(190) null,
  urls_api_nearby_projects text         null,
  urls_web_discover        text         null,
  urls_web_location        text         null
, localized_name TEXT NULL);
CREATE TABLE project
(
  id                                        int                                 not null
    primary key,
  state                                     varchar(20)                         null,
  url_project                               varchar(190)                        null,
  url_project_short                         varchar(190)                        null,
  name                                      varchar(190)                        null,
  country                                   varchar(20)                         null,
  creator_id                                int                                 null,
  location_id                               int                                 null,
  category_id                               int                                 null,
  created_at                                int                                 null,
  deadline                                  int                                 null,
  updated_at                                int                                 null,
  state_changed_at                          int                                 null,
  successful_at                             int                                 null,
  launched_at                               int                                 null,
  goal                                      decimal(20, 4)                      null,
  pledged                                   decimal(12, 4)                      null,
  currency                                  varchar(20)                         null,
  currency_symbol                           varchar(20)                         null,
  usd_pledged                               decimal(20, 10)                     null,
  static_usd_rate                           decimal(20, 10)                     null,
  backers_count                             int                                 null,
  comments_count                            int                                 null,
  updates_count                             int                                 null,
  spotlight                                 varchar(20)                         null,
  staff_pick                                varchar(20)                         null,
  blurb                                     text                                null,
  currency_trailing_code                    varchar(20)                         null,
  disable_communication                     varchar(20)                         null,
  photo_url                                 text                                null,
  profile_background_color                  varchar(190)                        null,
  profile_background_image_opacity          decimal(12, 4)                      null,
  profile_blurb                             text                                null,
  profile_id                                int                                 null,
  profile_link_background_color             varchar(20)                         null,
  profile_link_text                         text                                null,
  profile_link_text_color                   varchar(20)                         null,
  profile_link_url                          text                                null,
  profile_name                              varchar(190)                        null,
  profile_project_id                        int                                 null,
  profile_should_show_feature_image_section varchar(20)                         null,
  profile_show_feature_image                varchar(20)                         null,
  profile_state                             varchar(20)                         null,
  profile_state_changed_at                  int                                 null,
  profile_text_color                        varchar(20)                         null,
  slug                                      varchar(190)                        null,
  url_rewards                               varchar(190)                        null,
  url_updates                               varchar(190)                        null,
  video_id                                  int                                 null,
  video_url_high                            varchar(190)                        null,
  video_url_webm                            varchar(190)                        null,
  video_height                              int                                 null,
  video_width                               int                                 null,
  video_status                              varchar(20)                         null,
  file_name                                 varchar(190)                        null,
  last_modification                         timestamp default CURRENT_TIMESTAMP not null, deleted_comments int NULL,
  constraint project_url_project_uindex
  unique (url_project)
);
CREATE TABLE reward
(
  id                     int            not null
    primary key,
  project_id             int            null,
  title                  varchar(190)   null,
  reward                 text           null,
  title_for_backing_tier varchar(190)   null,
  shipping_preference    varchar(190)   null,
  description            text           null,
  shipping_summary       text           null,
  `limit`                int            null,
  estimated_delivery_on  int            null,
  ends_at                int            null,
  starts_at              int            null,
  updated_at             int            null,
  shipping_enabled       varchar(20)    null,
  backers_count          int            null,
  remaining              int            null,
  minimum                decimal(12, 4) null,
  urls_api_reward        text           null
);
CREATE TABLE `update`
(
  project_id          int  not null,
  post_unix_timestamp int  null,
  update_number       int  not null,
  title               text null,
  primary key (project_id, update_number)
);
CREATE TABLE "comments"
(
    id int PRIMARY KEY NOT NULL,
    projectid int NOT NULL,
    user_id varchar(100),
    user_name varchar(190),
    badge text,
    body text,
    post_date date
);
CREATE TABLE creator_external_url
        (
          creator_id int,
          href       text,
          target     text,
          link_text  text,
          PRIMARY KEY (creator_id, href)
        );
CREATE TABLE reward_item
(
    id INTEGER,
    item_project_id INTEGER,
    reward_id INTEGER,
    quantity INTEGER,
    position INTEGER,
    item_taxable VARCHAR,
    item_amount TEXT,
    item_edit_path TEXT,
    item_id INTEGER,
    item_name TEXT
);
CREATE TABLE creator_bio
(
    creator_id int PRIMARY KEY,
    location_name text,
    location_id int
);
CREATE INDEX reward_project_id_index
  on reward (project_id);
CREATE UNIQUE INDEX comments_projectid_id_uindex ON "comments" (projectid, id);
CREATE INDEX project_creator_id_index ON project (creator_id);
CREATE TRIGGER `UpdateLastModified`
  after update
  on project
  for each row
begin
  update project
  set last_modification = CURRENT_TIMESTAMP
  where id = old.id;
end;
CREATE INDEX location_displayable_name_index ON location (displayable_name);
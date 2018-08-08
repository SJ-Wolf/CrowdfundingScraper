CREATE TABLE loan (
  id                                                   int(11) primary key,
  name                                                 varchar(190),
  status                                               varchar(190),
  funded_amount                                        float,
  basket_amount                                        float,
  paid_amount                                          float,
  image_id                                             int(11),
  image_template_id                                    int(11),
  video_id                                             int(11),
  video_youtube_id                                     varchar(190),
  video_title                                          text,
  video_thumbnail_image_id                             int(11),
  activity                                             varchar(190),
  sector                                               varchar(190),
  use                                                  text,
  delinquent                                           tinyint(1),
  country_code                                         varchar(190),
  country                                              varchar(190),
  town                                                 varchar(190),
  geo_level                                            varchar(190),
  geo_pairs                                            varchar(190),
  geo_type                                             varchar(190),
  partner_id                                           int(11),
  posted_date                                          datetime,
  planned_expiration_date                              datetime,
  loan_amount                                          float,
  lender_count                                         int(11),
  borrower_count                                       int(11),
  currency_exchange_loss_amount                        float,
  bonus_credit_eligibility                             tinyint(1),
  terms_disbursal_date                                 datetime,
  terms_disbursal_currency                             varchar(190),
  terms_disbursal_amount                               float,
  terms_repayment_interval                             varchar(190),
  terms_repayment_term                                 int(11),
  terms_loan_amount                                    float,
  terms_loss_liability_nonpayment                      varchar(190),
  terms_loss_liability_currency_exchange               varchar(190),
  terms_loss_liability_currency_exchange_coverage_rate float,
  funded_date                                          datetime,
  paid_date                                            datetime,
  journal_total_entries                                int(11),
  journal_total_bulk_entries                           int(11),
  translator_byline                                    varchar(190),
  translator_image_id                                  int(11),
  scrape_time                                          datetime
);
CREATE TABLE theme (
  loan_id int(11),
  theme   varchar(190),
  primary key (loan_id, theme)
);
CREATE TABLE loan_lender (
  loan_id   int(11),
  lender_id varchar(190),
  primary key (loan_id, lender_id)
);
CREATE TABLE lender (
  lender_id         varchar(190) primary key,
  country_code      varchar(190),
  image_id          int(11),
  image_template_id int(11),
  invitee_count     int(11),
  inviter_id        varchar(190),
  loan_because      text,
  loan_count        int(11),
  member_since      datetime,
  name              varchar(190),
  occupation        varchar(190),
  occupational_info text,
  personal_url      varchar(190),
  uid               varchar(190),
  whereabouts       text
);
CREATE TABLE tag (
  loan_id int(11),
  name    varchar(190),
  primary key (loan_id, name)
);
CREATE TABLE payment (
  id                            int(11) primary key,
  loan_id                       int(11),
  amount                        float,
  processed_date                datetime,
  settlement_date               datetime,
  rounded_local_amount          int(11),
  currency_exchange_loss_amount float,
  local_amount                  float
);
CREATE TABLE local_payment (
  loan_id      int(11),
  order_number int(11),
  due_date     datetime,
  amount       float,
  primary key (loan_id, order_number)
);
CREATE TABLE scheduled_payment (
  loan_id      int(11),
  order_number int(11),
  due_date     datetime,
  amount       float,
  primary key (loan_id, order_number)
);
CREATE TABLE funding_trend (
  loan_id       int(11),
  status        varchar(190),
  funded_amount float,
  basket_amount float,
  paid_amount   float,
  delinquent    tinyint(1),
  lender_count  int(11),
  scrape_time   datetime,
  primary key (loan_id, scrape_time)
);
CREATE TABLE description (
  loan_id  int(11),
  language varchar(190),
  text     text,
  primary key (loan_id, language)
);
CREATE TABLE borrower (
  loan_id      int(11),
  order_number int(11),
  first_name   varchar(190),
  last_name    varchar(190),
  gender       varchar(190),
  pictured     tinyint(1),
  primary key (loan_id, order_number)
);
CREATE UNIQUE INDEX loan_lender_lender_id_loan_id_uindex ON loan_lender (lender_id, loan_id);
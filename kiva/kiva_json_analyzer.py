import logging


def add_column_to_list_of_dictionaries(data, column, value):
    for i in range(len(data)):
        data[i][column] = value


def fix_date_columns_in_list_of_dictionaries(data, date_columns):
    for i in range(len(data)):
        for col in date_columns:
            if data[i].get(col) is not None and data[i].get(col)[-1] == 'Z':
                data[i][col] = data[i][col][:-1]


def add_order_number_column_to_list_of_dictionaries(data):
    for i in range(len(data)):
        data[i]['order_number'] = i + 1


def rename_column_in_list_of_dictionaries(data, old_column, new_column):
    for i in range(len(data)):
        data[i][new_column] = data[i].pop(old_column)


def analyze_loans_lenders_data(loans_lenders):
    logging.info('Analyzing loans_lenders')
    flattened_loans_lenders = []
    for loans_lenders_row in loans_lenders:
        loan_id = loans_lenders_row['id']
        lender_ids = loans_lenders_row['lender_ids']
        if lender_ids is None:
            # flattened_loans_lenders.append(dict(loan_id=loan_id, lender_id="no lender"))
            pass
        else:
            for lender in lender_ids:
                flattened_loans_lenders.append(dict(loan_id=loan_id, lender_id=lender))

    # key should correspond to the table names in Kiva schema
    return dict(loan_lender=flattened_loans_lenders)


def analyze_loans_data(loans, scrape_time):
    logging.info('Analyzing loans')

    all_borrowers = []
    all_descriptions = []
    all_loans = []
    all_local_payments = []
    all_payments = []
    all_scheduled_payments = []
    all_tags = []
    all_themes = []

    for loan_row in loans:
        borrowers = loan_row.pop('borrowers')
        add_column_to_list_of_dictionaries(data=borrowers, column='loan_id', value=loan_row['id'])
        add_order_number_column_to_list_of_dictionaries(data=borrowers)

        description = []
        for language in loan_row['description']['languages']:
            description.append(dict(loan_id=loan_row['id'],
                                    language=language,
                                    text=loan_row['description']['texts'][language]))
        del loan_row['description']

        image = loan_row.pop('image')
        loan_row['image_id'] = image['id']
        loan_row['image_template_id'] = image['template_id']

        journal_totals = loan_row.pop('journal_totals')
        loan_row['journal_total_entries'] = journal_totals['entries']
        loan_row['journal_total_bulk_entries'] = journal_totals['bulkEntries']

        location = loan_row.pop('location')
        loan_row['country'] = location['country']
        loan_row['country_code'] = location['country_code']
        loan_row['town'] = location.get('town')
        loan_row['geo_pairs'] = location['geo']['pairs']
        loan_row['geo_type'] = location['geo']['type']
        loan_row['geo_level'] = location['geo']['level']

        payments = loan_row.pop('payments')
        add_column_to_list_of_dictionaries(data=payments, column='loan_id', value=loan_row['id'])
        rename_column_in_list_of_dictionaries(data=payments, old_column='payment_id', new_column='id')

        try:
            tags = loan_row.pop('tags')
        except KeyError:
            tags = []
        add_column_to_list_of_dictionaries(data=tags, column='loan_id', value=loan_row['id'])

        terms = loan_row.pop('terms')
        loan_row['terms_disbursal_amount'] = terms.get('disbursal_amount')
        loan_row['terms_disbursal_currency'] = terms.get('disbursal_currency')
        loan_row['terms_disbursal_date'] = terms.get('disbursal_date')
        loan_row['terms_loan_amount'] = terms.get('loan_amount')
        loan_row['terms_repayment_interval'] = terms.get('repayment_interval')
        loan_row['terms_repayment_term'] = terms.get('repayment_term')
        loan_row['terms_loss_liability_currency_exchange'] = terms['loss_liability'].get('currency_exchange')
        loan_row['terms_loss_liability_currency_exchange_coverage_rate'] = terms['loss_liability'].get('currency_exchange_coverage_rate')
        loan_row['terms_loss_liability_nonpayment'] = terms['loss_liability'].get('nonpayment')

        local_payments = terms['local_payments']
        add_column_to_list_of_dictionaries(data=local_payments, column='loan_id', value=loan_row['id'])
        add_order_number_column_to_list_of_dictionaries(data=local_payments)

        scheduled_payments = terms['scheduled_payments']
        add_column_to_list_of_dictionaries(data=scheduled_payments, column='loan_id', value=loan_row['id'])
        add_order_number_column_to_list_of_dictionaries(data=scheduled_payments)

        if 'themes' in loan_row.keys():
            if loan_row['themes'] is not None:
                themes = [dict(theme=x) for x in loan_row['themes']]
                add_column_to_list_of_dictionaries(data=themes, column='loan_id', value=loan_row['id'])
            else:
                themes = []
            del loan_row['themes']
        else:
            themes = []

        try:
            translator = loan_row.pop('translator')
        except KeyError:
            translator = None
        if translator is not None:
            loan_row['translator_byline'] = translator.get('byline')
            try:
                loan_row['translator_image_id'] = translator.get('image')
            except KeyError:
                loan_row['translator_image_id'] = None
        else:
            loan_row['translator_byline'] = None
            loan_row['translator_image_id'] = None

        try:
            video = loan_row.pop('video')
        except KeyError:
            video = None
        if video is not None:
            loan_row['video_id'] = video.get('id')
            loan_row['video_youtube_id'] = video.get('youtubeId')
            loan_row['video_title'] = video.get('title')
            loan_row['video_thumbnail_image_id'] = video.get('thumbnailImageId')
        else:
            loan_row['video_id'] = None
            loan_row['video_youtube_id'] = None
            loan_row['video_title'] = None
            loan_row['video_thumbnail_image_id'] = None

        all_borrowers += borrowers
        all_descriptions += description
        all_loans += [loan_row]
        all_local_payments += local_payments
        all_payments += payments
        all_scheduled_payments += scheduled_payments
        all_tags += tags
        all_themes += themes
    add_column_to_list_of_dictionaries(all_loans, 'scrape_time', scrape_time)
    fix_date_columns_in_list_of_dictionaries(all_loans, date_columns=(
        'posted_date', 'planned_expiration_date', 'terms_disbursal_date', 'funded_date', 'paid_date', 'scrape_time'))
    fix_date_columns_in_list_of_dictionaries(all_scheduled_payments, date_columns=('due_date',))
    fix_date_columns_in_list_of_dictionaries(all_local_payments, date_columns=('due_date',))
    fix_date_columns_in_list_of_dictionaries(all_payments, date_columns=('processed_date', 'settlement_date'))
    # keys should correspond to the table names in Kiva schema
    data = dict(borrower=all_borrowers,
                description=all_descriptions,
                loan=all_loans,
                local_payment=all_local_payments,
                payment=all_payments,
                scheduled_payment=all_scheduled_payments,
                tag=all_tags,
                theme=all_themes)
    return data


def analyze_lenders_data(lenders):
    logging.info('Analyzing lenders')
    for i in range(len(lenders)):
        image = lenders[i].pop('image')
        lenders[i]['image_id'] = image['id']
        lenders[i]['image_template_id'] = image['template_id']
    fix_date_columns_in_list_of_dictionaries(lenders, ('member_since',))

    # key should correspond to the table names in Kiva schema
    return dict(lender=lenders)

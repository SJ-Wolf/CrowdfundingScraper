from unused_scripts import db_connections
import time
from lxml import html
import codecs


class Rewards:
    failed_projects_output_file = None

    def output_html(self, f_name='output.html'):
        f = codecs.open(f_name, 'wb', encoding='utf-8')
        f.write(self.html_source)
        f.close()

    @staticmethod
    def only_numerics(seq):
        return filter(type(seq).isdigit, seq.split(".")[0])

    def __init__(self, html_source, projectid):
        try:
            # track if something went wrong scraping
            self.failed = True
            tree = html.fromstring(html_source)
            self.html_source = html_source
            self.tree = tree
            self.projectid = projectid

            # check if the project is purged
            purged_section = tree.xpath('//div[@id="purged_project"]')
            if len(purged_section) > 0:
                self.purged = True
                return
            else:
                self.purged = False
            reward_section = tree.xpath('//ul[@class="list mt2"]')[0]
            self.reward_levels = int(reward_section.attrib['data-reward-count'])
            reward_trees = tree.xpath('//div[contains(@class, "NS_backer_rewards__reward")]')
            assert len(reward_trees) == self.reward_levels
            # initialize individual reward variables
            self.amount_pledged = [None] * self.reward_levels
            self.backer_limit = [None] * self.reward_levels
            self.description = [None] * self.reward_levels
            self.backer_count = [None] * self.reward_levels
            self.delivery = [None] * self.reward_levels
            self.shipping_note = [None] * self.reward_levels

            for i, reward in enumerate(reward_trees):
                reward_text = reward.xpath("child::h5/text()")[0]
                self.amount_pledged[i] = Rewards.only_numerics(reward_text)
                backer_text = reward.xpath("child::p/span/span/text()")[0]
                self.backer_count[i] = Rewards.only_numerics(backer_text)
                self.description[i] = "".join(reward.xpath('child::div[contains(@class, "desc")]/p/text()')).strip()
                try:
                    self.delivery[i] = reward.xpath('child::div[@class="shipping-wrap"]//time')[0].attrib['datetime']
                except:
                    pass
                try:
                    backer_text = reward.xpath('child::*//span[@class="limited-number"]/text()')[0]
                    self.backer_limit[i] = int(backer_text.split(' of')[1].split(')')[0].replace(',', ''))
                except:
                    pass
                self.shipping_note[i] = "".join(
                    reward.xpath('child::*//div[@class="NS_backer_rewards__shipping"]/span/text()')).strip()
                if self.shipping_note[i] == '': self.shipping_note[i] = None
                if self.description[i] == '': self.description[i] = None
            self.failed = False  # yay!
        except:
            # projects MAY fail if they have been canceled or copyrighted
            # purged projects will? also end up here
            if Rewards.failed_projects_output_file is not None:
                Rewards.failed_projects_output_file.write(str(projectid) + "\n")

    '''
    returns a list of dictionaries where the keys are columns in the reward table of the kickstarter database
    specifically projectid, backer_limit, description, backer_count, delivery, and shipping_note
    returns None if project does not exist or has no rewards; this can be checked before calling this by looking
    at self.purged and self.reward_levels
    '''

    def get_rewards(self):
        try:
            if not self.failed and not self.purged and self.reward_levels > 0:
                rewards = []
                for i in range(self.reward_levels):
                    rewards.append(dict())
                    rewards[i]['projectid'] = self.projectid
                    rewards[i]['amount_required'] = self.amount_pledged[i]
                    rewards[i]['backer_limit'] = self.backer_limit[i]
                    rewards[i]['description'] = self.description[i]
                    rewards[i]['backer_count'] = self.backer_count[i]
                    rewards[i]['delivery'] = self.delivery[i]
                    rewards[i]['shipping_note'] = self.shipping_note[i]
            else:  # no rewards or project does not exist
                rewards = None
        except:
            raise Exception()
        return rewards


def upload_rewards_from_html_database(html_database, offset=0, chunk_size=10000, output_failed_projects=True):
    if output_failed_projects:
        Rewards.failed_projects_output_file = open('failed_reward_project_ids.txt', 'wb')
    print
    "Getting existing rewards"
    t1 = time.time()
    kickstarter_db = db_connections.get_fungrosencrantz_schema('kickstarter')
    existing_project_ids = set([row['projectid'] for row in kickstarter_db.query(
        'select distinct projectid from reward')])
    print
    '\ttook {0}'.format(time.time() - t1)

    while True:
        # have to reconnect or else it will lose connection after a while
        kickstarter_db = db_connections.get_fungrosencrantz_schema('kickstarter')
        print
        "Getting html from database, from {0} to {1}".format(offset, offset + chunk_size)
        t1 = time.time()
        results = html_database.query('select projectid, html, url from reward_html limit {0}, {1}'.format(offset, chunk_size))
        print
        '\ttook {0}'.format(time.time() - t1)
        print
        "Parsing html"
        t1 = time.time()
        rewards = []
        # see if the offset is past the end of the database
        end_of_database = True
        for index, row in enumerate(results):
            end_of_database = False
            if row['projectid'] not in existing_project_ids:
                r = Rewards(row['html'], row['projectid']).get_rewards()
                if r is not None:
                    rewards += r
        # break if offset is past the end of the database
        if end_of_database:
            print
            "Nothing to parse: end of database"
            break
        print
        '\ttook {0}'.format(time.time() - t1)
        print
        "Uploading projects"
        t1 = time.time()
        kickstarter_db['reward'].insert_many(ensure=False, rows=rewards)
        print
        '\ttook {0}'.format(time.time() - t1)

        offset += chunk_size

    if output_failed_projects:
        Rewards.failed_projects_output_file.close()


def run():
    html_database = db_connections.get_intermediate_db()
    upload_rewards_from_html_database(html_database, chunk_size=10000)


if __name__ == "__main__":
    run()

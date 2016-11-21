from __future__ import division
import itertools
from mrjob.job import MRJob, MRStep
import json
from collections import defaultdict, Counter
import heapq


class TopList(list):
    def __init__(self, 
                 max_size, 
                 num_position=0):
        """
        Just like a list, except 
        the append method adds 
        the new value to the 
        list only if it is larger
        than the smallest value 
        (or if the size of 
        the list is less than 
        max_size). 
        
        If each element of the list 
        is an int or float, uses 
        that value for comparison. 
        If the elements in the list 
        are lists or tuples, uses the 
        list_position element of the 
        list or tuple for the 
        comparison.
        """
        self.max_size = max_size
        self.pos = num_position
        
    def _get_key(self, x):
        if isinstance(x, (list, tuple)):
            return x[self.pos]
        else:
            return x
        
    def append(self, val):
        if len(self) < self.max_size:
            heapq.heappush(self, val)
        else:
            lowest_val = self._get_key(self[0])
            current_val = self._get_key(val)
            if current_val > lowest_val:
                heapq.heapreplace(self, val)
                
    def final_sort(self):
        return sorted(self, 
                      key=self._get_key, 
                      reverse=True)
    
    
class TopicPageRank(MRJob):
    MRJob.SORT_VALUES = True
    def configure_options(self):
        super(TopicPageRank, 
              self).configure_options()

        self.add_passthrough_option(
            '--reduce.tasks', 
            dest='reducers', 
            type='int',
            help="""number of reducers
            to use. Controls the hash
            space of the custom
            partitioner""")
        
        self.add_passthrough_option(
            '--iterations', 
            dest='iterations',
            default=5,
            type='int',
            help="""number of iterations
            to perform.""")
        
        self.add_passthrough_option(
            '--damping_factor', 
            dest='d', 
            default=.85,
            type='float',
            help="""Is the damping
            factor. Must be between
            0 and 1.""")
        
        self.add_passthrough_option(
            '--smart_updating', 
            dest='smart_updating', 
            type='str',
            default="False",
            help="""Can be True or
            False. If True, all updates
            to the new PR will take into
            account the value of the old
            PR.""")
        
        self.add_passthrough_option(
            '--return_top_k', 
            dest='return_top_k', 
            type='int',
            default=100,
            help="""Returns the results
            with the top k highest 
            PageRank scores.""")
        
    def clean_init(self):
        # Lazy mode
        self.topic_map = {'24': '9', '25': '7', '26': '1', '27': '1', '20': '3', '21': '9', '22': '4', '23': '6', '28': '7', '29': '1', '4': '5', '8': '8', '59': '2', '58': '2', '55': '7', '54': '8', '57': '9', '56': '6', '51': '5', '50': '7', '53': '7', '52': '1', '88': '5', '89': '4', '82': '2', '83': '4', '80': '5', '81': '1', '86': '3', '87': '8', '84': '4', '85': '7', '3': '10', '7': '10', '100': '8', '39': '8', '38': '4', '33': '1', '32': '1', '31': '3', '30': '7', '37': '6', '36': '1', '35': '7', '34': '5', '60': '10', '61': '8', '62': '8', '63': '4', '64': '10', '65': '4', '66': '3', '67': '1', '68': '10', '69': '6', '2': '3', '6': '8', '99': '5', '98': '1', '91': '3', '90': '5', '93': '4', '92': '1', '95': '10', '94': '9', '97': '7', '96': '9', '11': '6', '10': '1', '13': '6', '12': '2', '15': '3', '14': '9', '17': '10', '16': '1', '19': '1', '18': '8', '48': '10', '49': '10', '46': '1', '47': '7', '44': '1', '45': '5', '42': '9', '43': '10', '40': '3', '41': '4', '1': '10', '5': '5', '9': '2', '77': '1', '76': '4', '75': '2', '74': '10', '73': '2', '72': '4', '71': '2', '70': '3', '79': '4', '78': '4'}
        
    def clean_data(self, _, lines):
        key, value = lines.split("\t")
        value = json.loads(value.replace("'", '"'))
        links = value.keys()
        values = {"PR":1,"links":links,"topic":self.topic_map[str(key)]}
        yield (str(key), values)
        
    def mapper_init(self):
        self.values = {"***n_nodes_topics": defaultdict(int),
                       "**Distribute_topics": defaultdict(int)}
        self.n_reducers = self.options.reducers
    
    def mapper(self, key, line):
        n_reducers = self.n_reducers
        key_hash = hash(key)%n_reducers
        
        # Perform a node count each time
        PR = line["PR"]
        links = line["links"]
        topic = line["topic"]
        n_links = len(links)
        
        # If it is not a dangling node
        # distribute its PR to the 
        # other links.
        if n_links:
            PR_to_send = PR/n_links
            for link in links:
                link_hash = hash(link)%n_reducers
                yield (int(link_hash), (link, 
                                   PR_to_send))
        # If it is a dangling node, 
        # distribute its PR to all
        # other links
        else:
            self.values["**Distribute_topics"][topic] += PR
            
        # Pass original node onward
        yield (int(key_hash), (key, line))

    def mapper_final(self):
        # Push special keys to each unique hash
        for key, value in self.values.items():
            for k in range(self.n_reducers):
                yield (int(k), (key, value))
                
            
    def reducer_init(self):
        # Lazy mode
        self.topic_map = {'24': '9', '25': '7', '26': '1', '27': '1', '20': '3', '21': '9', '22': '4', '23': '6', '28': '7', '29': '1', '4': '5', '8': '8', '59': '2', '58': '2', '55': '7', '54': '8', '57': '9', '56': '6', '51': '5', '50': '7', '53': '7', '52': '1', '88': '5', '89': '4', '82': '2', '83': '4', '80': '5', '81': '1', '86': '3', '87': '8', '84': '4', '85': '7', '3': '10', '7': '10', '100': '8', '39': '8', '38': '4', '33': '1', '32': '1', '31': '3', '30': '7', '37': '6', '36': '1', '35': '7', '34': '5', '60': '10', '61': '8', '62': '8', '63': '4', '64': '10', '65': '4', '66': '3', '67': '1', '68': '10', '69': '6', '2': '3', '6': '8', '99': '5', '98': '1', '91': '3', '90': '5', '93': '4', '92': '1', '95': '10', '94': '9', '97': '7', '96': '9', '11': '6', '10': '1', '13': '6', '12': '2', '15': '3', '14': '9', '17': '10', '16': '1', '19': '1', '18': '8', '48': '10', '49': '10', '46': '1', '47': '7', '44': '1', '45': '5', '42': '9', '43': '10', '40': '3', '41': '4', '1': '10', '5': '5', '9': '2', '77': '1', '76': '4', '75': '2', '74': '10', '73': '2', '72': '4', '71': '2', '70': '3', '79': '4', '78': '4'}
        self.d = self.options.d
        smart = self.options.smart_updating
        if smart == "True":
            self.smart = True
        elif smart == "False":
            self.smart = False
        else:
            msg = """--smart_updating should 
                       be True or False"""
            raise Exception(msg)
        self.to_distribute_topics = Counter()
        self.n_nodes_topics = Counter()

    def reducer(self, hash_key, combo_values):
        gen_values = itertools.groupby(combo_values, 
                                       key=lambda x:x[0])
        # Hask key is a pseudo partitioner.
        # Unpack old keys as separate
        # generators.
        for key, values in gen_values:
            total = 0
            node_info = None

            for key, val in values:
                # If the val is a number,
                # accumulate total.
                if isinstance(val, (float, int)):
                    total += val
                elif isinstance(val, defaultdict):
                    if key == "**Distribute_topics":
                        for k in val:
                            val[k] = val[k]/self.n_nodes_topics[k]
                        self.to_distribute_topics += Counter(val)
                    elif key == "***n_nodes_topics":
                        self.n_nodes_topics += Counter(val)
                else:
                    if key == "**Distribute_topics":
                        continue
                    # Means that the key-value
                    # pair corresponds to a node
                    # of the form. 
                    # {"PR": ..., "links: [...]}
                    node_info = val
            # Most keys will reference a node, so
            # put this check first.
            if node_info:
                old_pr = node_info["PR"]
                distribute = self.to_distribute_topics.get(node_info["topic"]) or 0
                pr = total + distribute
                decayed_pr = self.d * pr
                teleport_pr = 1-self.d
                new_pr = decayed_pr + teleport_pr
                if self.smart:
                    # Use old PR to inform
                    # new PR.
                    diff = abs(new_pr - old_pr)
                    percent_diff = diff/old_pr
                    if percent_diff < .3:
                        new_pr = .8*new_pr + .2*old_pr
                node_info["PR"] = new_pr
                yield (key, node_info)
            else:
                if key in ["***n_nodes_topics", "**Distribute_topics"]:
                    continue
                # Track dangling nodes.
                yield (key, {"PR": 1, 
                             "links": [],
                             "topic": self.topic_map[key]})
                
    def decrease_file_size(self, key, value):
        val = value["PR"]
        if val > .1:
            yield ("top", (key, round(val,4)))
    
    def collect_init(self):
        top_k = self.options.return_top_k
        self.top_vals = TopList(top_k, 1)
    
    def collect(self, key, values):
        for val in values:
            self.top_vals.append(val)
            
    def collect_final(self):
        for val in self.top_vals.final_sort():
            yield val

    def steps(self):
        iterations = self.options.iterations
        mr_steps = (
            [MRStep(mapper_init=self.clean_init,
                    mapper=self.clean_data)] 
            +
            [MRStep(
                   mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   mapper_final=self.mapper_final,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer
                    )]*iterations
            +
            [MRStep(mapper=self.decrease_file_size,
                    reducer_init=self.collect_init,
                    reducer=self.collect,
                    reducer_final=self.collect_final)]
                    )
        return mr_steps


if __name__ == "__main__":
    TopicPageRank.run()
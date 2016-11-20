from __future__ import division, print_function
from sys import stderr
import itertools
from mrjob.job import MRJob, MRStep
import json

class SimplePageRank(MRJob):    
    def configure_options(self):
        super(SimplePageRank, 
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
        
    def clean_data(self, _, lines):
        key, value = lines.split("\t")
        value = json.loads(value.replace("'", '"'))
        links = value.keys()
        values = {"PR":1,"links":links}
        yield (str(key), values)
        
    def mapper_init(self):
        self.values = {"***n_nodes": 0,
                       "**Distribute": 0}
        self.n_reducers = self.options.reducers
    
    def mapper(self, key, line):
        
        n_reducers = self.n_reducers
        key_hash = hash(key)%n_reducers
        # Handles special keys
        if key in ["**Distribute", 
                   "***n_nodes"]:
            val = line
            self.values[key] += val
            raise StopIteration
        
        # Perform a node count each time
        self.values["***n_nodes"] += 1
        PR = line["PR"]
        links = line["links"]
        n_links = len(links)
        
        # If it is not a dangling node
        # distribute its PR to the 
        # other links.
        if n_links:
            PR_to_send = PR/n_links
            for link in links:
                link_hash = hash(link)%n_reducers
                yield (link_hash, (link, 
                                   PR_to_send))
        # If it is a dangling node, 
        # distribute its PR to all
        # other links
        else:
            self.values["**Distribute"] += PR
            
        # Pass original node onward
        yield (key_hash, (key, line))

    def mapper_final(self):
        # Push special keys to each unique hash
        for key, value in self.values.items():
            for k in range(self.n_reducers):
                yield (k, (key, value))
            
    def reducer_init(self):
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
        self.to_distribute = None
        self.n_nodes = None
        self.total_pr = None

    def reducer(self, hash_key, combo_values):
        gen_values = itertools.groupby(combo_values, 
                                       key=lambda x:x[0])
        # Because we are using hash_key as a pseudo 
        # partitioner, we have to unpack the unique
        # keys within each generator to mimic standard
        # mrjob functionality.
        # gen_values should be treated as the standard
        # generator made available in the reduce step
        for key, values in gen_values:
            total = 0
            node_info = None

            for key, val in values:
                # If the val is a number,
                # accumulate total.
                if isinstance(val, (float, int)):
                    total += val
                else:
                    # Means that the key-value
                    # pair corresponds to a node
                    # of the form. 
                    # {"PR": ..., "links: [...]}
                    node_info = val
            # Most keys will reference a node, so
            # put this check first.
            if node_info:
                old_pr = node_info["PR"]
                distribute = self.to_distribute or 0
                pr = total + distribute
                decayed_pr = self.d * pr
                teleport_pr = 1-self.d
                new_pr = decayed_pr + teleport_pr
                if self.smart:
                    # If the new value is less than
                    # 30% different than the old
                    # value, set the new PR to be
                    # 80% of the new value and 20% 
                    # of the old value.
                    diff = abs(new_pr - old_pr)
                    percent_diff = diff/old_pr
                    if percent_diff < .3:
                        new_pr = .8*new_pr + .2*old_pr
                node_info["PR"] = new_pr
                yield (key, node_info)
            elif key == "***n_nodes":
                self.n_nodes = total
            elif key == "**Distribute":
                # Because the node_count and
                # the total distribution are 
                # eventually consistent, a
                # simple correction for any early
                # discrepancies is a good fix
                self.to_distribute = total/self.n_nodes
            else:
                # Catches dangling nodes. 
                # Not a special key and no
                # node information.
                # The only time this should run
                # is when dangling nodes are 
                # discovered during the first
                # iteration. By making them
                # explicitly tracked, the mapper
                # can handle them from now on.
                yield ("**Distribute", total)
                yield ("***n_nodes", 1)
                yield (key, {"PR": 1, 
                             "links": []})

    def steps(self):
        iterations = self.options.iterations
        mr_steps = ([MRStep(mapper=self.clean_data)] 
                    +
                    [MRStep(
                           mapper_init=self.mapper_init,
                           mapper=self.mapper,
                           mapper_final=self.mapper_final,
                           reducer_init=self.reducer_init,
                           reducer=self.reducer
                            )]*iterations
                   )
        return mr_steps


if __name__ == "__main__":
    SimplePageRank.run()
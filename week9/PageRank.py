
from __future__ import print_function
from mrjob.job import MRJob
from mrjob.job import MRStep
from mrjob.protocol import JSONProtocol
from sys import stderr

class PageRank(MRJob):
    INPUT_PROTOCOL = JSONProtocol
    
    def configure_options(self):
        super(PageRank, 
              self).configure_options()

        self.add_passthrough_option(
            '--n_nodes', 
            dest='n_nodes', 
            type='float',
            help="""number of nodes 
            that have outlinks""")
    
    def mapper_init(self):
        n = self.options.n_nodes
        self.n_nodes = float(n)
    
    def mapper(self, key, lines):
        # Handles special keys
        # Calculate new Total PR
        # each iteration
        if key in ["****Total PR"]:
            raise StopIteration
        if key in ["**Distribute", "***n_nodes"]:
            # !!! This is where the special
            # hash to the same reducer code
            # will need to go.
            yield (key, lines)
            raise StopIteration
        # Handles the first time the 
        # mapper is called. The lists
        # are converted to dictionaries 
        # with default PR values.
        if isinstance(lines, list):
            default_PR = 1/self.n_nodes
            lines = {"links":lines, 
                     "PR": default_PR}
            # Also perform a node count
            yield ("***n_nodes", 1)
        PR = lines["PR"]
        links = lines["links"]
        n_links = len(links)
        # Pass node onward
        yield (key, lines)
        # Track total PR in system
        yield ("****Total PR", PR)
        # If it is not a dangling node
        # distribute its PR to the 
        # other links.
        if n_links:
            PR_to_send = PR/n_links
            for link in links:
                yield (link, PR_to_send)
        else:
            # !!! This is also where the special
            # hash must go.
            yield ("**Distribute", PR)

    def reducer_init(self):
        self.to_distribute = None
        self.n_nodes = None
        self.total_pr = None
    
    def reducer(self, key, values):
        total = 0
        node_info = None
        
        for val in values:
            if isinstance(val, (float, 
                                int)):
                total += val
            else:
                node_info = val
                
        if node_info:
            to_distribute = self.to_distribute or 0
            new_pr = total + to_distribute
            node_info["PR"] = new_pr
            yield (key, node_info)
        elif key == "****Total PR":
            self.total_pr = total
            yield (key, total)
        elif key == "***n_nodes":
            self.n_nodes = total
            yield (key, total)
        elif key == "**Distribute":
            extra_mass = total
            excess_pr = self.total_pr - 1
            weight = extra_mass - excess_pr
            self.to_distribute = weight/self.n_nodes
        else:
            # The only time this should run
            # is when dangling nodes are 
            # discovered during the first
            # iteration. By making them
            # explicitly tracked, the mapper
            # can handle them from now on.
            yield ("**Distribute", total)
            yield ("***n_nodes", 1)
            yield (key, {"PR": total, 
                         "links": []})
            
    def steps(self):
        mr_steps = [MRStep(mapper_init=self.mapper_init,
                           mapper=self.mapper,
                           reducer_init=self.reducer_init,
                           reducer=self.reducer)]*5
        return mr_steps
        
if __name__ == "__main__":
    PageRank.run()


class Replication:
    def __init__(self, role='master', replication_ID='8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb', offset=0):
        self.role = role
        self.replication_ID = replication_ID
        self.offset = offset
        pass

    def initial_replication_ID_and_Offset(self):
        return {
            'role' : "$82\r\nrole:master\n",
            'master_replid' : f"master_replid:{self.replication_ID}\n",
            'master_repl_offset' : f"master_repl_offset:{self.offset}\r\n",
        }
    
    # b'$82\r\nrole:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0\r\n'
# final b'$8\r\nrole:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0\r\n'
$TTL 86400
@       IN      SOA     PowerEdge.oitc.com. root.oitc.com. (
        0 ; serial
        84600 ; refresh
        7200 ; retry
        846000 ; expire
        84000 ) ; minimum
                        IN      NS      PowerEdge.oitc.com.
PowerEdge.oitc.com.     IN      A       192.168.1.100
hadoop-1.oitc.com.      IN      A       192.168.1.101
hadoop-2.oitc.com.      IN      A       192.168.1.102

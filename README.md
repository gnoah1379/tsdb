# Query

> select 
> rate(cpu_usage_total_seconds{host='host1'}[1h]) / cpu_machine *1000
> from abc
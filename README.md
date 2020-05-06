# pyBalancer
A python implemention of application load balancer

## installation:
pip3 install -r requirements.txt

## usage:
python3 pyBalancer.py

## configuration:
```
{
    "listen_on":<port to listen for incoming req>,
    "health_check_interval":<time in seconds>,
    "workers":[
        {
            "ip_address":"<server 1 ip>",
            "port":<server 1 port>,
            "health_check_port":<server 1 health check port>,
            "health_check_path":"<server 1 health check path>",
            "health_check_status_code":<server 1 health check status code>
        }
        {
            "ip_address":"<server 2 ip>",
            "port":<server 2 port>,
            "health_check_port":<server 2 health check port>,
            "health_check_path":"<server 2 health check path>",
            "health_check_status_code":<server 2 health check status code>
        }

    ]
}
```
## sample configuration:
```
{
    "listen_on":5000,
    "health_check_interval":20
    "workers":[
        {
            "ip_address":"localhost",
            "port":3000,
            "health_check_port":3000,
            "health_check_path":"/",
            "health_check_status_code":200
        },
        {
            "ip_address":"localhost",
            "port":3001,
            "health_check_port":3001,
            "health_check_path":"/",
            "health_check_status_code":200
        }

    ]

}
```

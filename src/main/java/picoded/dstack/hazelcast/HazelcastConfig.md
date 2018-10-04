# Connection configuration variables accepted by HazelcastLoader

This is expected to be in a json-map format, that will be passed to the HazelcastLoader class,
and is listed as the following.

When used as a provider, this would be under the `connection` object

## Common configuration settings (regardless of mode)

| Name              | Type    | Default Value | Description                                            |
|-------------------|---------|---------------|--------------------------------------------------------|
| groupName         | String  |               | **compulsory** Group name of the cluster               |
| mode              | String  | server        | HazelcastInstance mode, either as 'server' or 'client' |

## Server mode specific setting (ignored in client mode)

| Name              | Type    | Default Value | Description                                            |
|-------------------|---------|---------------|--------------------------------------------------------|
| multicast         | boolean | true          | Multicast scan used to detect other servers on startup |
| port              | int     | 5900          | Server port                                            |
| portAutoIncrement | boolean | true          | Auto increment port mode                               |
| memberTcpList     | list    | []            | TCP or TCP:port list of members to first connect to    |


# Sky Spy Watch

This software is a prototype designed to convert flight path data collected from [Virtual Radar Server](https://github.com/vradarserver/vrs) and filter for potential surveillance activity. The principal method for flagging surveillance activity is a count of the number of 90 degree heading changes, scored differentially based on altitude. A flight or aircraft that is flagged by this software is merely "interesting", and worth more scrutiny regarding its possible use in surveillance than average aircraft. The idea and basic principals were [presented](https://www.nstarpost.com/news/defcon-25-spies-in-the-skies/) at DEFCON 25 on July 29th, 2017 in Las Vegas.

The back-end software is written in Python3 and should work on Debian Stretch (stable). Some testing and development on the Ansible playbook for installation is necessary to migrate to Debian Stretch. SkySpyWatch requires Redis 3.2+, recent versions of RabbitMQ, and MongoDB. A preliminary Ansible playbook and Vagrant configuration is included, but still requires additional testing. SkySpyWatch is being designed with multi-core and multi-server usage in mind. The web map (front end) is currently designed to use AWS S3 for static content storage and delivery, but could support other static storage or web hosting with some minor modifications. SkySpyWatch requires a decently powerful multi-core server or set of servers to process all global data from ADS-B Exchange. It is possible to reduce the area covered (and CPU requirements) by limiting the area pulled from Virtual Radar Server.

SkySpyWatch requires some key data to be useful. This data is available from third parties that have particular terms and conditions that must be adhered to--please refer to their websites to ensure you are compliant. Please also contribute to these data providers as much as practical so they keep their data as open as possible.

[ADS-B Exchange](https://www.adsbexchange.com "ADS-B Exchange") - provides [real time flight tracking data](https://www.adsbexchange.com/data/)

[ourairports.com](http://ourairports.com) - provides [locations](http://ourairports.com/data/) on airports which are used to help identify if aircraft have landed. This data may also be used to help filter out loops and other patterns near airports / heliports that is less likely to be surveillance activity.

There are currently severe bugs in this software that will be documented and fixed in the future. In general, the documentation for SkySpyWatch is very limited and it is not easy to get this up and running.

There are a number of ways that you can contribute to this effort:
- Set up an ADS-B receiver and configure it to feed data to [ADS-B Exchange](https://www.adsbexchange.com), particularly if you are in a rural area, outside of the US, or somewhere with weak coverage.
- Donate money to [ADS-B Exchange](https://www.adsbexchange.com/donate/) so they can continue to make data available
- Pull requests!
- Advice, particularly on multiprocessing in Python3
- Thoughts and comments on a machine learning approach
- Improving other components that this application relies on - particularly [Virtual Radar Server](https://github.com/vradarserver/vrs) so that it is more Linux friendly
- Donations of money - donation info is at [nstarpost.com](https://www.nstarpost.com/support-nsp/)
- Donations of server time


Getting started (with Vagrant):
- vagrant up
- vagrant ssh
- some not yet documented / automated steps to get VirtualRadarServer working
- set some credentials in vrscreds.json
- start some python scripts
- eventually get it running

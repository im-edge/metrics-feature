Icinga Graphing
===============

Icinga Graphing wants to offer an unexcited pleasantly relaxed performance
graphing experience. Implemented as a thin and modern abstraction layer based
on matured technology it puts its main focus on robustness and ease of use.

Performance Graphing should "just work" out of the box. We do not assume that
our users carry a data science degree. Based on our field experience with Open
Source monitoring solutions, we make strong assumptions on what your preferences
might be. While it of course allows for customization, it ships with opinionated,
preconfigured data retention rules. You CAN care, but you do not have to.

Technology Choices
------------------

This software has been built on the shoulders of giants, namely the following
ones:

* [RRDtool](https://oss.oetiker.ch/rrdtool/): used to be THE OpenSource industry
  standard for high performance time series data graphing. It's no longer "cool"
  enough, as it has no REST API and it requires you to read it's manual. We love
  the raw rendering power and simplicity of this tool, that's why it builds one
  of the core parts of **Icinga Graphing**.

* [Redis](https://redis.io): an Open Source in-memory data structure store. We
  use it as a local cache and message broker. We use it as a buffer for burst
  situations and to share data amongst different processes. We love its stream
  capabilities, and also it's LUA VM.

* [PHP](https://www.php.net): even if considered not being "cool" by many, it
  is one of the most popular and fastest general-purpose scripting languages.
  It undertook enormous changes over the years, while trying hard to not break
  compatibility.

* [ReactPHP](https://reactphp.org/): relatively small, battle-tested library.
  Allows writing event-driven code based on non-blocking I/O in PHP.

When not to use Icinga Graphing
-------------------------------

While you might want to use this software for many purposes, we do not aim to
be everybody's darling. We are driven by the needs of the [Icinga](https://icinga.com)
monitoring software, and this leads to specific design decisions.

Chances are good that it could also perfectly fit your requirements. It might
not when you:

* want to track performance data in related to the geographical position of a
  moving entity? Use TimescaleDB or a similar product
* want to store a bunch of often-changing tags combined with your performance
  data? You might want to check out InfluxDB

Just want to store performance data without employing a data scientist? Give
**Icinga Graphing** a try!

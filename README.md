# b-social/event-processor

A libary to process events making a number of choices

- Using stuartsierra components
- Event processing is wrapped in a jdbc transaction   
- Configured using environment variables from configurati
- Logs using cambium

Allows for multiple processors configured in different ways. 

Now defers to the config-agnostic KSB version of this library, providing a backwards-compatible wrapper that uses configurati.

## Install

Add the following to your `project.clj` file:

```clj
[b-social/event-processor "<VERSION>"]
```

Ensure that you're set up to access our internal maven libraries (as of 1.x.x this library is no longer usable outside of Kroo codebases due to internal dependencies).

## License

Copyright © 2020 Kroo Ltd

Distributed under the terms of the 
[MIT License](http://opensource.org/licenses/MIT).

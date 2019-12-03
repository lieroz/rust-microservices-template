# rust-microservices-template
This is a simple project that can be used as a reference on how to organize microservice architecture using Rust language. Was made for personal usage.

### TODO
## Here are points that must be fixed, they were made due to my incompetence, laziness and lack of experience

- [ ] Fix error handling (there are lots of unwrap calls, change for appropriate error handling)
- [ ] Move common code between workspaces into separate project (structs, functions, etc)
- [ ] Make functions more granular for testing purposes (there are indeed a lot of extra large hot functions)
- [ ] Signal handling (quick working example was used, need to be tested and understood)
- [ ] Change Redis for PostgreSQL or equivalent (tried to make things simple, but turned out to be bad design and a lot of code that is not handled appropriately)
- [ ] Move JSON validation schemas to text files (right now there are hardcoded)

# redmap

Distributed golang map-reduce jobs using Go plugins.

**Note:** this is very much a wip and just for learning purposes.

### Roadmap

- [x] Worker proto
- [x] Worker client
- [x] Worker server
- [x] Manager proto
- [ ] Manager client
- [ ] Manager server
- [ ] Worker CLI
- [ ] Master CLI
- [ ] Send jobs via CLI
- [ ] Handle node/job failures
- [ ] Compile plugins before sending them as jobs
- [ ] Auth for attaching nodes
- [x] Reuse connections on worker clients
- [ ] Reporting via CLI/web monitor
- [ ] Implement retry policies

## License

MIT, see [LICENSE](/LICENSE).
# CCHDO Snapshotter
For transferring things to the UCSD library.
It was recently (as of writing this, check the git blame) updated to use uv as the project management tool.
It was also updated to have an entrypoint script with the same name.
Once it is published on pypi, you should be able to use any python tool runner:

```
uvx cchdo.snapshotter
```

Until that is done, clone the repository and the following should work

```
uv run cchdo.snapshotter
```

Once the snapshot is created, it should be a valid BagIt bag.
As of writing this (see git blame again), re-running the snapshot after it has been bagged will corrupt/nest the bags.
Don't do this.

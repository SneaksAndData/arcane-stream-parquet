# Debugging guide

# Connecting to the pod
We are using a [distroless image](https://github.com/GoogleContainerTools/distroless),
so the production image does not contain a shell or any other tools that we can use to debug the application.

To debug the application, we need to use a debug image.

To be able to do that, we need to go to the .container directory and add the debug tag to the image name in the
final stage of the build in the [Dockerfile](https://github.com/SneaksAndData/arcane-stream-parquet/blob/main/.container/Dockerfile):

```Dockerfile
FROM gcr.io/distroless/java21-debian12:debug
```

When it's done, we can build the image using the Build Container Image workflow, which will publish
the image to the GitHub container registry. Then, we can run the application using the debug image.

> [!NOTE]
> While the action pushes the image to the GitHub Container Registry, it's advisable not to push the debug image to
> the AWS public ECR repository to avoid overpopulating it with too many debug tags.

When we are done and the stream is running using the debug image, we can connect to the pod using kubectl or other
tools.

# Collecting stack traces
If the stream runner is hanging, we can collect a thread dump to see what is happening in the application.
To do that, we can use the jstack command available in the debug image.

> [!NOTE]
> The paths here and below are valid for now and can be changed in the future. If you are using a different image,
> please consult the documentation for the image you are using.

If Arcane is running in a container with PID 1, we can use the following command to collect a thread dump:

```
/usr/lib/jvm/temurin21_jdk_{arm64, amd64}/bin/jstack 1 > thread_dump.txt
```

When collected, thread dump can be moved to the local machine using the `kubectl cp` command.

We can easily analyze the thread dump using
[Intellij IDEA](https://www.jetbrains.com/help/idea/analyzing-external-stacktraces.html) or other tools.

# Collecting heap dumps
If the stream runner is using too much memory, we can collect a heap dump to see what the memory is used for.

As in the example above, we use the jmap command available in the debug image and collect a heap dump for the PID 1:

```
/usr/lib/jvm/temurin21_jdk_{arm64, amd64}/bin/jmap -dump:format=b,file=heap.hprof 1
```

> [!WARNING]  
> Ensure that you have enough disk space in the pod to collect the heap dump.

We can analyze the heap dump using [Eclipse MAT](https://www.eclipse.org/mat/),
[Intellij IDEA](https://www.jetbrains.com/help/idea/create-a-memory-snapshot.html#-cww8bx_8) or other tools.

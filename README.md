# docker_runner

usage

```rust
let docker = Docker::connect_with_socket_defaults().unwrap();
// change the max_container_running_time to desire value
let dr = DockerRunner::new(docker, 1, "runner_container".into(), "yes".into(), 10);
// call this to free images
dr.clear_images_by_whitelist().await.unwrap();
dr.run("busybox:latest", vec!["sleep", "100"])
    .await
    .unwrap();
dr.run("busybox:latest", vec!["sleep", "100"])
    .await
    .unwrap();
dr.run("busybox:latest", vec!["sleep", "100"])
    .await
    .unwrap();
assert_eq!(3, dr.list_runner_containers().await.unwrap().len());
tokio::time::sleep(Duration::from_secs(3)).await;
// call this to free containers
dr.clear_timeout_containers().await.unwrap();
assert_eq!(0, dr.list_runner_containers().await.unwrap().len());
```

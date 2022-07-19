use bollard::container::{
    AttachContainerOptions, AttachContainerResults, Config, CreateContainerOptions,
    ListContainersOptions, StartContainerOptions,
};
use bollard::errors::Error;
use bollard::image::{CreateImageOptions, ListImagesOptions, RemoveImageOptions};
use bollard::models::{
    ContainerCreateResponse, ContainerSummary, EventMessage, HostConfig, ImageSummary, Mount,
    MountTypeEnum, PortMap,
};
use bollard::system::EventsOptions;
pub use bollard::Docker;
pub use bollard::models::PortBinding;
use chrono::{Duration, Utc};
use futures_util::{Stream, StreamExt, TryStreamExt};
use std::io::{stdout, Write};

use std::collections::HashMap;

/// Maxmium time in seconds of a container that can stay running
pub static MAX_CONTAINER_RUNNING_TIME: i64 = 60 * 60 * 24;

/// Maxmium container running count
pub static MAX_CONTAINERS: usize = 10;

#[derive(Clone)]
pub struct DockerRunner {
    pub docker: Docker,
    pub max_container_running_time: i64,
    pub container_label_key: String,
    pub container_label_value: String,
    pub max_containers: usize,
}

impl DockerRunner {
    pub fn new(
        docker: Docker,
        max_container_running_time: i64,
        container_label_key: String,
        container_label_value: String,
        max_containers: usize,
    ) -> Self {
        return DockerRunner {
            docker,
            max_container_running_time,
            container_label_key,
            container_label_value,
            max_containers,
        };
    }

    pub async fn remove_image_by_name(&self, name: String) -> Result<Vec<String>, Error> {
        let filters: HashMap<&str, Vec<&str>> = HashMap::new();
        let options = Some(ListImagesOptions {
            all: false,
            filters,
            ..Default::default()
        });
        let images = self.docker.list_images(options).await?;
        let mut removed_hash = vec![];
        for image in images {
            if image.repo_tags.contains(&name) {
                let remove_options = Some(RemoveImageOptions {
                    ..Default::default()
                });
                removed_hash.push(image.id.clone());
                self.docker
                    .remove_image(&image.id, remove_options, None)
                    .await?;
            }
        }
        Ok(removed_hash)
    }

    pub async fn list_images(&self) -> Result<Vec<ImageSummary>, Error> {
        let filters: HashMap<&str, Vec<&str>> = HashMap::new();
        let options = Some(ListImagesOptions {
            all: false,
            filters,
            ..Default::default()
        });
        Ok(self.docker.list_images(options).await?)
    }

    /// Create container
    pub async fn run(
        &self,
        image: &str,
        cmd: Option<Vec<&str>>,
        mounts: Option<Vec<(String, String)>>,
        extra_labels: Option<Vec<(String, String)>>,
        port_bindings: Option<PortMap>,
    ) -> Result<ContainerCreateResponse, Box<dyn std::error::Error>> {
        let filters: HashMap<&str, Vec<&str>> = HashMap::new();
        let options = Some(ListImagesOptions {
            all: false,
            filters,
            ..Default::default()
        });
        let images = self.docker.list_images(options).await?;
        // Pull image if image is not already exitsting
        if !images
            .iter()
            .map(|image_summary| image_summary.repo_tags.clone())
            .flatten()
            .collect::<Vec<String>>()
            .contains(&image.to_string())
        {
            let options = Some(CreateImageOptions {
                from_image: image,
                ..Default::default()
            });

            let mut stream = self.docker.create_image(options.clone(), None, None);
            while let Some(msg) = stream.try_next().await? {
                log::info!("Pulling image: {:?}", msg);
            }
        }
        let mut labels: HashMap<String, String> = HashMap::new();
        labels.insert(
            self.container_label_key.clone(),
            self.container_label_value.clone(),
        );
        for (tag_key, tag_value) in extra_labels.unwrap_or(vec![]) {
            labels.insert(tag_key, tag_value);
        }
        let host_config = HostConfig {
            cap_add: Some(vec!["SYS_ADMIN".into()]),
            auto_remove: Some(true),
            // 1GB limit
            memory: Some(1073741824),
            // for container access localhost service
            extra_hosts: Some(vec!["host.docker.internal:host-gateway".into()]),
            mounts: Some(
                mounts
                    .unwrap_or(vec![])
                    .iter()
                    .map(|(source, target)| Mount {
                        target: Some(String::from(target)),
                        source: Some(String::from(source)),
                        typ: Some(MountTypeEnum::BIND),
                        consistency: Some(String::from("default")),
                        ..Default::default()
                    })
                    .collect(),
            ),
            port_bindings,
            ..Default::default()
        };
        let cfg = Config {
            image: Some(image),
            cmd,
            labels: Some(
                labels
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str()))
                    .collect::<HashMap<&str, &str>>(),
            ),
            host_config: Some(host_config),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        };
        let options: Option<CreateContainerOptions<&str>> = None;
        let resp = self.docker.create_container(options, cfg).await?;
        self.docker
            .start_container(&resp.id, None::<StartContainerOptions<String>>)
            .await?;

        let AttachContainerResults {
            mut output,
            input: _,
        } = self
            .docker
            .attach_container(
                &resp.id,
                Some(AttachContainerOptions::<String> {
                    stdout: Some(true),
                    stderr: Some(true),
                    stream: Some(true),
                    ..Default::default()
                }),
            )
            .await?;

        // set stdout in raw mode so we can do tty stuff
        let mut stdout = stdout();

        tokio::spawn(async move {
            // pipe docker attach output into stdout
            while let Some(Ok(output)) = output.next().await {
                stdout.write_all(output.into_bytes().as_ref()).unwrap();
                stdout.flush().unwrap();
            }
        });
        Ok(resp)
    }

    /// Clear image by whitlist
    pub async fn clear_images_by_whitelist(
        &self,
        whitelist: Vec<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let filters: HashMap<&str, Vec<&str>> = HashMap::new();
        let options = Some(ListImagesOptions {
            all: false,
            filters,
            ..Default::default()
        });
        let whitelist_map = whitelist
            .iter()
            .map(|hash| (hash.clone(), true))
            .collect::<HashMap<&str, bool>>();
        let images = self.docker.list_images(options).await?;
        for image in images {
            if !whitelist_map.contains_key(image.id.as_str()) {
                let remove_options = Some(RemoveImageOptions {
                    ..Default::default()
                });

                if let Err(e) = self
                    .docker
                    .remove_image(&image.id, remove_options, None)
                    .await
                {
                    log::warn!("Failed to clear old image {}, {:?}", image.id, e);
                } else {
                    log::info!("Cleared old image {}", image.id);
                }
            }
        }
        Ok(())
    }

    /// Clear image by tag
    pub async fn clear_images_by_tag(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut filters = HashMap::new();
        let label = format!(
            "{}={}",
            self.container_label_key, self.container_label_value
        );
        filters.insert("label", vec![label.as_str()]);
        let options = Some(ListImagesOptions {
            all: true,
            filters,
            ..Default::default()
        });
        let images = self.docker.list_images(options).await?;
        for image in images {
            let remove_options = Some(RemoveImageOptions {
                noprune: true,
                ..Default::default()
            });

            if let Err(e) = self
                .docker
                .remove_image(&image.id, remove_options, None)
                .await
            {
                log::warn!("Failed to clear old image {}, {:?}", image.id, e);
            } else {
                log::info!("Cleared old image {}", image.id);
            }
        }
        Ok(())
    }

    /// Clear timeout containers and stopped containers
    pub async fn clear_timeout_containers(&self) -> Result<(), Box<dyn std::error::Error>> {
        for container_info in self.list_runner_containers(None).await? {
            let container_created_timestamp = container_info.created.unwrap_or(0);
            let now = chrono::offset::Local::now().timestamp();
            if (now - container_created_timestamp) > self.max_container_running_time {
                for name in container_info.names.unwrap() {
                    // FIXME: The return value of name is /charming_leakey with a / at the front,
                    // but the stop_container method expect a name without /
                    if let Err(e) = self
                        .docker
                        .stop_container(&name.trim_start_matches("/"), None)
                        .await
                    {
                        log::warn!("Failed to clear container {}, {:?}", name, e);
                    }
                }
                log::info!(
                    "Clear container {} for timeout",
                    container_info.id.unwrap_or("No id found".into())
                );
            } else {
                log::info!(
                    "Skip running container {}",
                    container_info.id.unwrap_or("No id found".into())
                );
            }
        }
        Ok(())
    }

    /// Clear containers by label
    pub async fn clear_containers_by_labels(
        &self,
        labels: Option<Vec<String>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for container_info in self.list_runner_containers(labels).await? {
            for name in container_info.names.unwrap() {
                // FIXME: The return value of name is /charming_leakey with a / at the front,
                // but the stop_container method expect a name without /
                if let Err(e) = self
                    .docker
                    .stop_container(&name.trim_start_matches("/"), None)
                    .await
                {
                    log::warn!("Failed to clear container {}, {:?}", name, e);
                }
            }
            log::info!(
                "Clear container {} by labels",
                container_info.id.unwrap_or("No id found".into())
            );
        }
        Ok(())
    }

    /// Clear containers by deadline
    pub async fn clear_containers_by_deadline(
        &self,
        deadline_key: String,
        deadline: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for container_info in self.list_runner_containers(None).await? {
            let labels = container_info.labels.unwrap_or(HashMap::new());
            if let Some(ddl) = labels.get(&deadline_key) {
                let ddl_label = ddl.parse::<u64>().unwrap_or(0_u64);
                if ddl_label != 0_u64 && ddl_label < deadline {
                    for name in container_info.names.unwrap_or(vec![]) {
                        // FIXME: The return value of name is /charming_leakey with a / at the front,
                        // but the stop_container method expect a name without /
                        if let Err(e) = self
                            .docker
                            .stop_container(&name.trim_start_matches("/"), None)
                            .await
                        {
                            log::warn!("Failed to clear container {}, {:?}", name, e);
                        }
                    }
                    log::info!(
                        "Clear container {} by deadline",
                        container_info.id.unwrap_or("No id found".into())
                    );
                }
            }
        }
        Ok(())
    }

    /// Clear all containers
    pub async fn clear_all_containers(&self) -> Result<(), Box<dyn std::error::Error>> {
        for container_info in self.list_runner_containers(None).await? {
            for name in container_info.names.unwrap() {
                // FIXME: The return value of name is /charming_leakey with a / at the front,
                // but the stop_container method expect a name without /
                if let Err(e) = self
                    .docker
                    .stop_container(&name.trim_start_matches("/"), None)
                    .await
                {
                    log::warn!("Failed to clear container {}, {:?}", name, e);
                }
            }
            log::info!(
                "Clear container {} for reset",
                container_info.id.unwrap_or("No id found".into())
            );
        }
        Ok(())
    }

    pub async fn list_runner_containers(
        &self,
        labels: Option<Vec<String>>,
    ) -> Result<Vec<ContainerSummary>, Box<dyn std::error::Error>> {
        let mut filters = HashMap::new();
        let mut labels_vec = vec![format!(
            "{}={}",
            self.container_label_key, self.container_label_value
        )];
        for label in labels.unwrap_or(vec![]) {
            labels_vec.push(label)
        }
        filters.insert("label".to_string(), labels_vec);
        filters.insert("status".to_string(), vec!["running".to_string()]);
        let opts: ListContainersOptions<String> = ListContainersOptions {
            all: true,
            filters,
            ..Default::default()
        };

        Ok(self.docker.list_containers(Some(opts)).await?)
    }

    pub async fn events(
        &self,
        filters: HashMap<String, Vec<String>>,
    ) -> Result<impl Stream<Item = Result<EventMessage, Error>>, Box<dyn std::error::Error>> {
        Ok(self.docker.events(Some(EventsOptions::<String> {
            since: Some(Utc::now() - Duration::minutes(20)),
            until: None,
            filters,
        })))
    }
}

#[cfg(test)]
mod tests {

    use simplelog::*;

    use super::*;
    use bollard::models::PortBinding;
    use std::collections::HashMap;
    #[tokio::test]
    async fn test_portmap() {
        CombinedLogger::init(vec![TermLogger::new(
            LevelFilter::Info,
            simplelog::Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        )])
        .unwrap();
        let docker = Docker::connect_with_socket_defaults().unwrap();
        let dr = DockerRunner::new(docker, 1, "runner_container".into(), "yes".into(), 10);
        let mut portmap = HashMap::new();
        portmap.insert(
            "8080/tcp".into(),
            Some(vec![PortBinding {
                host_ip: Some("0.0.0.0".into()),
                host_port: Some("8080".into()),
            }]),
        );
        dr.run(
            "xbgxwh/credentials:1.0.0",
            Some(vec!["sleep", "100"]),
            None,
            Some(vec![("id".to_string(), "1".to_string())]),
            Some(portmap),
        )
        .await
        .unwrap();
        log::info!("{:?}", dr.list_images().await.unwrap());
    }
    #[tokio::test]
    async fn test_list_images() {
        CombinedLogger::init(vec![TermLogger::new(
            LevelFilter::Info,
            simplelog::Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        )])
        .unwrap();
        let docker = Docker::connect_with_socket_defaults().unwrap();
        let dr = DockerRunner::new(docker, 1, "runner_container".into(), "yes".into(), 10);
        log::info!("{:?}", dr.list_images().await.unwrap());
    }
    #[tokio::test]
    async fn test_clear_images() {
        CombinedLogger::init(vec![TermLogger::new(
            LevelFilter::Info,
            simplelog::Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        )])
        .unwrap();
        let docker = Docker::connect_with_socket_defaults().unwrap();
        let dr = DockerRunner::new(docker, 1, "runner_container".into(), "yes".into(), 10);
        dr.remove_image_by_name("xbgxwh/oracle_price:1.0.0".to_string())
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn test_events() {
        let docker = Docker::connect_with_socket_defaults().unwrap();
        let mut filters: HashMap<String, Vec<String>> = HashMap::new();
        filters.insert("event".into(), vec!["destroy".to_string()]);
        filters.insert("type".into(), vec!["container".to_string()]);
        let mut s = docker.events(Some(EventsOptions::<String> {
            since: Some(Utc::now() - Duration::minutes(20)),
            until: None,
            filters,
        }));
        while let Ok(event) = s.try_next().await {
            println!("{:?}", event);
        }
    }
    #[tokio::test]
    async fn test_runner_basic_functions() {
        CombinedLogger::init(vec![TermLogger::new(
            LevelFilter::Info,
            simplelog::Config::default(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        )])
        .unwrap();
        let docker = Docker::connect_with_socket_defaults().unwrap();
        let dr = DockerRunner::new(docker, 1, "runner_container".into(), "yes".into(), 10);
        // dr.clear_images_by_whitelist(vec![
        //     // helium miner
        //     "sha256:9f78fc7319572294768f78381ff58eef7c0e4d49605a9f994b2fab056463dce0",
        //     // oracle price
        //     "sha256:6150ce190b77e1077f7040d594f2303c28d96c135b25ad6e8c31cc4990ded3d4",
        //     // wrk
        //     "sha256:00af8c6b99adbadc465f42f9dcc8b0f10e397dbcae4fb71966126a0834f3a3f5",
        //     // github star
        //     "sha256:066003b681db10eca929503a4de0b4859468dceaaf57c30749fff77dd397bef9",
        // ])
        // .await
        // .unwrap();
        dr.run(
            "busybox:latest",
            Some(vec!["sleep", "100"]),
            None,
            Some(vec![("id".to_string(), "1".to_string())]),
            None,
        )
        .await
        .unwrap();
        dr.run(
            "busybox:latest",
            Some(vec!["sleep", "100"]),
            None,
            Some(vec![("id".to_string(), "1".to_string())]),
            None,
        )
        .await
        .unwrap();
        dr.run(
            "busybox:latest",
            Some(vec!["sleep", "100"]),
            None,
            Some(vec![("id".to_string(), "2".to_string())]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(3, dr.list_runner_containers(None).await.unwrap().len());
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        dr.clear_containers_by_labels(Some(vec!["id=1".into()]))
            .await
            .unwrap();
        assert_eq!(1, dr.list_runner_containers(None).await.unwrap().len());
        dr.run(
            "busybox:latest",
            Some(vec!["sleep", "100"]),
            None,
            Some(vec![("deadline".to_string(), "50".to_string())]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(2, dr.list_runner_containers(None).await.unwrap().len());
        dr.clear_containers_by_deadline("deadline".into(), 10)
            .await
            .unwrap();
        assert_eq!(2, dr.list_runner_containers(None).await.unwrap().len());
        dr.clear_containers_by_deadline("deadline".into(), 60)
            .await
            .unwrap();
        assert_eq!(1, dr.list_runner_containers(None).await.unwrap().len());
    }
}

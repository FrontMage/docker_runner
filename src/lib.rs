use bollard::container::{
    Config, CreateContainerOptions, ListContainersOptions, StartContainerOptions,
};
use bollard::image::{CreateImageOptions, ListImagesOptions, RemoveImageOptions};
use bollard::models::{ContainerCreateResponse, ContainerSummary, CreateImageInfo, HostConfig};
pub use bollard::Docker;
use futures_util::TryStreamExt;

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

    /// Create container
    pub async fn run(
        &self,
        image: &str,
        cmd: Option<Vec<&str>>,
    ) -> Result<ContainerCreateResponse, Box<dyn std::error::Error>> {
        let options = Some(CreateImageOptions {
            from_image: image,
            ..Default::default()
        });

        let mut stream = self.docker.create_image(options.clone(), None, None);
        while let Some(msg) = stream.try_next().await? {
            log::info!("Pulling image: {:?}", msg);
        }
        let mut labels: HashMap<&str, &str> = HashMap::new();
        labels.insert(&self.container_label_key, &self.container_label_value);
        let host_config = HostConfig {
            auto_remove: Some(true),
            ..Default::default()
        };
        let cfg = Config {
            image: Some(image),
            cmd,
            labels: Some(labels),
            host_config: Some(host_config),
            ..Default::default()
        };
        let options: Option<CreateContainerOptions<&str>> = None;
        let resp = self.docker.create_container(options, cfg).await?;
        self.docker
            .start_container(&resp.id, None::<StartContainerOptions<String>>)
            .await?;
        Ok(resp)
    }

    /// Clear image by whitlist
    pub async fn clear_images_by_whitelist(
        &self,
        whitelist: Vec<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let filters: HashMap<&str, Vec<&str>> = HashMap::new();
        let options = Some(ListImagesOptions {
            filters,
            ..Default::default()
        });
        let whitelist_map = whitelist
            .iter()
            .map(|hash| (hash.clone(), true))
            .collect::<HashMap<&str, bool>>();
        let images = self.docker.list_images(options).await?;
        for image in images {
            // FIXME: This should be a whitlist, now just exclude the helium miner
            if whitelist_map.contains_key(image.id.as_str()) {
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
        for container_info in self.list_runner_containers().await? {
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

    pub async fn list_runner_containers(
        &self,
    ) -> Result<Vec<ContainerSummary>, Box<dyn std::error::Error>> {
        let mut filters = HashMap::new();
        filters.insert(
            "label".to_string(),
            vec![format!(
                "{}={}",
                self.container_label_key, self.container_label_value
            )],
        );
        filters.insert("status".to_string(), vec!["running".to_string()]);
        let opts: ListContainersOptions<String> = ListContainersOptions {
            all: true,
            filters,
            ..Default::default()
        };

        Ok(self.docker.list_containers(Some(opts)).await?)
    }
}

#[cfg(test)]
mod tests {

    use simplelog::*;
    use std::time::Duration;

    use super::*;
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
        // dr.clear_images_by_whitelist().await.unwrap();
        dr.run("busybox:latest", Some(vec!["sleep", "100"]))
            .await
            .unwrap();
        dr.run("busybox:latest", Some(vec!["sleep", "100"]))
            .await
            .unwrap();
        dr.run("busybox:latest", Some(vec!["sleep", "100"]))
            .await
            .unwrap();
        assert_eq!(3, dr.list_runner_containers().await.unwrap().len());
        tokio::time::sleep(Duration::from_secs(3)).await;
        dr.clear_timeout_containers().await.unwrap();
        assert_eq!(0, dr.list_runner_containers().await.unwrap().len());
    }
}

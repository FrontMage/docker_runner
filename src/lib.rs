use bollard::container::{
    Config, InspectContainerOptions, ListContainersOptions, StopContainerOptions,
};
use bollard::models::ContainerSummary;
use bollard::Docker;

use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::image::CreateImageOptions;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures_util::stream;
use futures_util::stream::StreamExt;
use futures_util::TryStreamExt;
use std::collections::HashMap;
use tokio::runtime::Builder;

/// Maxmium time in seconds of a container that can stay running
pub static MAX_CONTAINER_RUNNING_TIME: i64 = 60 * 60 * 1;

/// All runner container must use this tag to filter out
pub static CONTAINER_LABEL: &'static str = "is_runner_container=true";

pub struct DockerRunner {
    pub docker: Docker,
    pub containers: Vec<ContainerSummary>,
}

impl DockerRunner {
    pub fn new(docker: Docker) -> Self {
        return DockerRunner {
            docker,
            containers: vec![],
        };
    }

    /// Create container
    pub async fn run(&self, image: &str, args: &str) -> Result<(), Box<dyn std::error::Error>> {
        // self.docker.create_container();
        unimplemented!("");
        Ok(())
    }

    /// Clear old images
    pub async fn clear_images(&self) -> Result<(), Box<dyn std::error::Error>> {
        unimplemented!("");
        Ok(())
    }

    /// Clear timeout containers and stopped containers
    pub async fn clear_timeout_containers(&self) -> Result<(), Box<dyn std::error::Error>> {
        for container_info in self.list_runner_containers().await? {
            let container_created_timestamp = container_info.created.unwrap_or(0);
            let now = chrono::offset::Local::now().timestamp();
            if (now - container_created_timestamp) > MAX_CONTAINER_RUNNING_TIME {
                // TODO: maybe sometimes container won't stop? Require force stop?
                for name in container_info.names.unwrap() {
                    self.docker.stop_container(&name, None).await?;
                }
                println!(
                    "Clear container {} for timeout",
                    container_info.id.unwrap_or("No id found".into())
                );
            } else {
                println!(
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
        filters.insert("label".to_string(), vec![CONTAINER_LABEL.to_string()]);
        let opts: ListContainersOptions<String> = ListContainersOptions {
            all: true,
            filters,
            ..Default::default()
        };

        Ok(self.docker.list_containers(Some(opts)).await?)
    }

    pub async fn list_running_containers(
        &self,
    ) -> Result<Vec<ContainerSummary>, Box<dyn std::error::Error>> {
        let mut filters = HashMap::new();
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

    use super::*;
    #[test]
    fn test_get_all_containers() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let docker = Docker::connect_with_socket_defaults().unwrap();
            let dr = DockerRunner::new(docker);
            dr.clear_timeout_containers().await.unwrap();
        });
    }
}

//! Node identity in `name@host:port` form (e.g. `node_a@10.0.0.1:9001`).

/// Identity of a node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId {
    name: String,
    host: String,
    port: u16,
}

impl NodeId {
    /// Parses `name@host:port`.
    pub fn parse(s: &str) -> Result<NodeId, NodeIdError> {
        let (name, addr) = s.split_once('@').ok_or(NodeIdError::MissingAt)?;
        let (host, port_str) = addr.rsplit_once(':').ok_or(NodeIdError::MissingPort)?;
        if name.is_empty() {
            return Err(NodeIdError::EmptyName);
        }
        if host.is_empty() {
            return Err(NodeIdError::EmptyHost);
        }
        let port = port_str.parse().map_err(|_| NodeIdError::InvalidPort)?;
        Ok(NodeId {
            name: name.into(),
            host: host.into(),
            port,
        })
    }

    /// Creates a node id from parts.
    pub fn new(name: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        NodeId {
            name: name.into(),
            host: host.into(),
            port,
        }
    }

    /// Returns a copy with a different port (used after binding to `:0`).
    pub fn with_port(&self, port: u16) -> Self {
        NodeId {
            name: self.name.clone(),
            host: self.host.clone(),
            port,
        }
    }

    /// Node name (`node_a`).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Host part (`10.0.0.1`).
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Port part (`9001`).
    pub fn port(&self) -> u16 {
        self.port
    }

    /// `host:port`.
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// `name@host:port`.
    pub fn to_full(&self) -> String {
        format!("{}@{}", self.name, self.address())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeIdError {
    MissingAt,
    MissingPort,
    EmptyName,
    EmptyHost,
    InvalidPort,
}

impl std::fmt::Display for NodeIdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeIdError::MissingAt => write!(f, "expected 'name@host:port'"),
            NodeIdError::MissingPort => write!(f, "expected 'host:port' in node id"),
            NodeIdError::EmptyName => write!(f, "node name must not be empty"),
            NodeIdError::EmptyHost => write!(f, "host must not be empty"),
            NodeIdError::InvalidPort => write!(f, "port must be a number"),
        }
    }
}

impl std::error::Error for NodeIdError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_node_id() {
        let id = NodeId::parse("node_a@10.0.0.1:9001").unwrap();
        assert_eq!(id.name(), "node_a");
        assert_eq!(id.host(), "10.0.0.1");
        assert_eq!(id.port(), 9001);
        assert_eq!(id.address(), "10.0.0.1:9001");
        assert_eq!(id.to_full(), "node_a@10.0.0.1:9001");
    }

    #[test]
    fn parses_localhost() {
        let id = NodeId::parse("node_b@127.0.0.1:9002").unwrap();
        assert_eq!(id.address(), "127.0.0.1:9002");
    }

    #[test]
    fn roundtrips_new() {
        let id = NodeId::new("n", "localhost", 1);
        assert_eq!(id.to_full(), "n@localhost:1");
        assert_eq!(NodeId::parse("n@localhost:1").unwrap(), id);
    }

    #[test]
    fn with_port_updates_only_port() {
        let id = NodeId::parse("node_a@127.0.0.1:0")
            .unwrap()
            .with_port(12345);
        assert_eq!(id, NodeId::parse("node_a@127.0.0.1:12345").unwrap());
    }

    #[test]
    fn rejects_missing_at() {
        assert_eq!(
            NodeId::parse("no_at_sign:9001"),
            Err(NodeIdError::MissingAt)
        );
    }

    #[test]
    fn rejects_missing_port() {
        assert_eq!(NodeId::parse("node@host"), Err(NodeIdError::MissingPort));
    }

    #[test]
    fn rejects_empty_name() {
        assert_eq!(NodeId::parse("@host:9001"), Err(NodeIdError::EmptyName));
    }

    #[test]
    fn rejects_empty_host() {
        assert_eq!(NodeId::parse("node@:9001"), Err(NodeIdError::EmptyHost));
    }

    #[test]
    fn rejects_invalid_port() {
        assert_eq!(
            NodeId::parse("node@host:abc"),
            Err(NodeIdError::InvalidPort)
        );
        assert_eq!(NodeId::parse("node@host:-1"), Err(NodeIdError::InvalidPort));
    }
}

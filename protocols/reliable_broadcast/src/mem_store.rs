use crate::{Errors, TrbStore};
use std::collections::{BTreeSet, HashMap};
use topos_core::uci::{Certificate, CertificateId, DigestCompressed, SubnetId};

/// Store implementation in RAM good enough for functional tests
/// Might need to split through a new layer of TRBState
/// between ReliableBroadcast and rocksdb
#[derive(Default, Clone)]
pub struct TrbMemStore {
    /// Global map of delivered and accepted certificates
    all_certs: HashMap<CertificateId, Certificate>,
    /// Mapping SubnetId -> Delivered certificated
    history: HashMap<SubnetId, BTreeSet<CertificateId>>,
    /// Consider for now that each TCE nodes is following all subnets
    tracked_digest: HashMap<SubnetId, BTreeSet<CertificateId>>,
    /// Digest received from elsewhere
    received_digest: HashMap<CertificateId, DigestCompressed>,
    /// List of the subnets that we're part of
    /// NOTE: Below is for later, for now we're considering
    /// being part of all subnets, so following digest of everyone
    followed_subnet: Vec<SubnetId>,
}

impl TrbMemStore {
    pub fn new(subnets: Vec<SubnetId>) -> TrbMemStore {
        let mut store = TrbMemStore::default();
        store.followed_subnet = subnets;
        for subnet in &store.followed_subnet {
            store.tracked_digest.insert(*subnet, BTreeSet::new());
            store.history.insert(*subnet, BTreeSet::new());
        }
        // Add the genesis
        store.all_certs.insert(0, Default::default());
        store
    }
}

impl TrbStore for TrbMemStore {
    // JAEGER START DELIVERY TRACE [ cert, peer ]
    fn apply_cert(&mut self, cert: &Certificate) -> Result<(), Errors> {
        // Add the entry in the history <SubnetId, CertId>
        let _ = self.add_cert_in_hist(&cert.initial_subnet_id, cert);

        // Add the cert into the history of each Terminal
        for call in &cert.calls {
            self.add_cert_in_hist(&call.terminal_subnet_id, &cert);
            self.add_cert_in_digest(&call.terminal_subnet_id, &cert.id);
        }

        Ok(())
    }

    fn add_cert_in_hist(&mut self, subnet_id: &SubnetId, cert: &Certificate) -> bool {
        self.all_certs.insert(cert.id.clone(), cert.clone());
        self.history
            .entry(subnet_id.clone())
            .or_default()
            .insert(cert.id.clone())
    }

    fn add_cert_in_digest(&mut self, subnet_id: &SubnetId, cert_id: &CertificateId) -> bool {
        self.tracked_digest
            .entry(subnet_id.clone())
            .or_default()
            .insert(cert_id.clone())
    }

    fn read_journal(
        &self,
        _subnet_id: SubnetId,
        _from_offset: u64,
        _max_results: u64,
    ) -> Result<(Vec<Certificate>, u64), Errors> {
        unimplemented!();
    }

    fn recent_certificates_for_subnet(
        &self,
        subnet_id: &SubnetId,
        _last_n: u64,
    ) -> Option<Vec<CertificateId>> {
        match self.history.get(subnet_id) {
            Some(subnet_certs) => Some(subnet_certs.iter().cloned().collect::<Vec<_>>()),
            _ => None,
        }
    }

    /// Compute and flush the digest for the given subnet
    fn flush_digest_view(&mut self, subnet_id: &SubnetId) -> Option<DigestCompressed> {
        match self.tracked_digest.get_mut(subnet_id) {
            Some(current_digest) => {
                let digest_compressed = Some(current_digest.iter().cloned().collect::<Vec<_>>());
                current_digest.clear();
                digest_compressed
            }
            _ => None,
        }
    }

    fn cert_by_id(&self, cert_id: &CertificateId) -> Result<Option<Certificate>, Errors> {
        match self.all_certs.get(cert_id) {
            Some(cert) => Ok(Some(cert.clone())),
            _ => Err(Errors::CertificateNotFound),
        }
    }

    // JAEGER END DELIVERY TRACE [ cert, peer ]
    fn new_cert_candidate(&mut self, cert: &Certificate, digest: &DigestCompressed) {
        self.received_digest.insert(cert.id, digest.clone());
    }

    ///
    /// Checks
    ///
    fn check_digest_inclusion(&self, cert: &Certificate) -> Result<(), Errors> {
        let received_digest = self.received_digest.get(&cert.id).unwrap();

        // Check that all cert in digest are in my history
        received_digest
            .iter()
            .all(|cert_id| self.cert_by_id(cert_id).is_ok());
        Ok(())
    }

    fn check_precedence(&self, cert: &Certificate) -> Result<(), Errors> {
        match self.cert_by_id(&cert.prev_cert_id) {
            Ok(Some(_)) => Ok(()),
            _ => Err(Errors::CertificateNotFound),
        }
    }

    fn clone_box(&self) -> Box<dyn TrbStore + Send> {
        Box::new(self.clone())
    }
}

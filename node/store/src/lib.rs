use rocksdb::{IteratorMode, ReadOptions, DB};
use std::sync::Arc;
use tce_trbp::{trb_store::TrbStore, Errors};
use tce_uci::{Certificate, CertificateId, DigestCompressed, SubnetId};

/// Configuration of RocksDB store
pub struct StoreConfig {
    pub db_path: Option<String>,
}

/// RocksDB based store
///
/// Data structure:
///     - certificates:
///         key - certificateId, value - certificate
///     - journal of delivered certificates:
///         key - composed from subnetId and offset, value - subnetId + certificateId
///
/// Implements TrbStore
#[derive(Clone)]
pub struct Store {
    db: Arc<DB>,
}

impl Store {
    pub fn new(config: StoreConfig) -> Self {
        let db_path = config.db_path.unwrap_or_else(|| "./default_db".into());
        let db = DB::open_default(db_path).unwrap();
        Self { db: Arc::new(db) }
    }
}

impl TrbStore for Store {
    fn apply_cert(&mut self, cert: &Certificate) -> Result<(), Errors> {
        self.db
            .put(Self::cert_key(&cert.id), bincode::serialize(&cert).unwrap())
            .expect("db save");
        Ok(())
    }

    fn add_cert_in_hist(&mut self, subnet_id: &SubnetId, cert: &Certificate) -> bool {
        let key = self.new_journal_key(subnet_id);
        let cert_key = Self::cert_key(&cert.id);
        self.db.put(key, cert_key).expect("db save");
        true
    }

    fn add_cert_in_digest(&mut self, _subnet_id: &SubnetId, _cert: &CertificateId) -> bool {
        unimplemented!("Please prefer using the TrbMemStore for now");
    }

    // Want also from one CertificateId as offset
    fn read_journal(
        &self,
        _subnet_id: SubnetId,
        _from_offset: u64,
        _max_results: u64,
    ) -> Result<(Vec<Certificate>, u64), Errors> {
        //todo
        Ok((vec![], 0u64))
    }

    fn get_cert(&self, _subnet_id: &SubnetId, _last_n: u64) -> Option<Vec<CertificateId>> {
        unimplemented!("Please prefer TrbMemStore for now");
    }

    fn cert_by_id(&self, cert_id: &CertificateId) -> Result<Option<Certificate>, Errors> {
        let mb_bin_cert = self.db.get(Self::cert_key(cert_id)).expect("db get");
        let mb_cert = mb_bin_cert
            .map(|bc| bincode::deserialize::<Certificate>(bc.as_ref()).expect("Cert deser"));

        Ok(mb_cert)
    }

    fn flush_digest_view(&mut self, _subnet_id: &SubnetId) -> Option<DigestCompressed> {
        unimplemented!("Please prefer using the TrbMemStore for now");
    }

    fn check_digest_inclusion(&self, _cert: &Certificate) -> Result<(), Errors> {
        unimplemented!("Please prefer using the TrbMemStore for now");
    }

    fn check_precedence(&self, cert: &Certificate) -> Result<(), Errors> {
        match self.cert_by_id(&cert.prev_cert_id) {
            Ok(Some(_)) => Ok(()),
            _ => Err(Errors::CertificateNotFound),
        }
    }

    fn new_cert_candidate(&mut self, _cert: &Certificate, _digest: &DigestCompressed) {
        unimplemented!("Please prefer using the TrbMemStore for now");
    }

    fn clone_box(&self) -> Box<dyn TrbStore + Send> {
        Box::new(self.clone())
    }
}

impl Store {
    fn cert_key(cert_id: &CertificateId) -> Vec<u8> {
        let mut key = b"cert:".to_vec();
        key.push(*cert_id as u8);
        //key.append(&mut cert_id.clone());
        key
    }

    fn new_journal_key(&mut self, subnet_id: &SubnetId) -> Vec<u8> {
        let mut key_prefix = b"journal:".to_vec();
        //key_prefix.append(&mut subnet_id);
        key_prefix.push(*subnet_id as u8);
        key_prefix.append(&mut b":".to_vec());

        // find last item for this subnet
        let mut ro = ReadOptions::default();
        ro.set_iterate_lower_bound(Self::add_offset(&key_prefix, 0));
        ro.set_iterate_upper_bound(Self::add_offset(&key_prefix, u64::MAX));
        let mut iter = self.db.iterator_opt(IteratorMode::End, ro);
        let offset = if let Some(a) = iter.next() {
            Self::extract_offset(a.0.as_ref().into()) + 1
        } else {
            0u64
        };

        Self::add_offset(&key_prefix, offset)
    }

    fn add_offset(key_prefix: &[u8], offset: u64) -> Vec<u8> {
        let mut key = key_prefix.to_owned();
        key.append(&mut format!("{:020}", offset).into_bytes());
        key
    }

    fn extract_offset(key: Vec<u8>) -> u64 {
        let str_key = String::from_utf8_lossy(key.as_ref());
        let parts = str_key
            .split(':')
            .map(|i| i.into())
            .collect::<Vec<String>>();

        if parts.len() > 2 {
            parts[2].parse().unwrap_or(0u64)
        } else {
            0u64
        }
    }
}

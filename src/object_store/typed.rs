use serde::{de::DeserializeOwned, Serialize};
use std::ops::Deref;

use super::*;

pub struct CborTyped<O> {
    backing: O,
}

impl<O> CborTyped<O> {
    pub fn new(backing: O) -> Self {
        Self { backing }
    }
}

impl<O> Deref for CborTyped<O> {
    type Target = O;

    fn deref(&self) -> &Self::Target {
        &self.backing
    }
}

impl<O> CborTyped<O>
where
    O: ObjectStore,
{
    pub async fn set_typed<T>(&self, key: String, t: &T) -> IoResult<()>
    where
        T: Serialize,
    {
        self.backing.set(key, ser(t)?).await
    }

    pub async fn get_typed<T>(&self, key: &str) -> IoResult<Option<T>>
    where
        T: DeserializeOwned,
    {
        match self.backing.get(key).await? {
            None => Ok(None),
            Some(read) => Ok(Some(de(&read)?)),
        }
    }
}

fn ser<T: Serialize>(t: &T) -> IoResult<Vec<u8>> {
    serde_cbor::to_vec::<T>(t).map_err(|e| {
        log::error!("Error serializing stored data: {}", e);
        IoError::InvalidData
    })
}

fn de<T: DeserializeOwned>(bytes: &[u8]) -> IoResult<T> {
    serde_cbor::from_slice::<T>(bytes).map_err(|e| {
        log::error!("Error deserializing stored data: {}", e);
        IoError::InvalidData
    })
}

/// Update the value at the provided key. May retry until successful.
pub async fn update_typed<T, R, F, O>(store: &O, key: &str, mut f: F) -> IoResult<R>
where
    O: ObjectStore,
    F: for<'v> FnMut(Option<T>) -> IoResult<(T, R)> + Send,
    T: Serialize + DeserializeOwned,
    R: Send,
{
    super::update(store, key, |read| {
        let t = read.map(|r| de(r)).transpose()?;
        let (new_t, r) = f(t)?;
        let bytes = ser(&new_t)?;
        Ok((bytes, r))
    })
    .await
}

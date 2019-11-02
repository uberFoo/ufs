use {
    crate::UfsUuid,
    jsonwebtoken as jwt,
    jwt::{decode, encode, Algorithm, Header, Validation},
    serde::{Deserialize, Serialize},
};

pub(crate) type JWT = String;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct UserClaims {
    pub(crate) iss: UfsUuid,
    pub(crate) sub: UfsUuid,
    pub(crate) jti: UfsUuid,
}

pub(crate) fn new_jwt(claims: UserClaims, secret: &[u8]) -> JWT {
    encode(&Header::default(), &claims, secret).expect("unable to create JWT")
}

const assert = require('assert');
const crypto = require('crypto');
const { errors } = require('arsenal');

const BucketInfo = require('arsenal').models.BucketInfo;
const bucketGet = require('../../../lib/api/bucketGet');
const bucketGetACL = require('../../../lib/api/bucketGetACL');
const bucketGetCors = require('../../../lib/api/bucketGetCors');
const bucketGetWebsite = require('../../../lib/api/bucketGetWebsite');
const bucketHead = require('../../../lib/api/bucketHead');
const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutACL = require('../../../lib/api/bucketPutACL');
const bucketPutCors = require('../../../lib/api/bucketPutCors');
const bucketPutWebsite = require('../../../lib/api/bucketPutWebsite');
const bucketDelete = require('../../../lib/api/bucketDelete');
const bucketDeleteCors = require('../../../lib/api/bucketDeleteCors');
const bucketDeleteWebsite = require('../../../lib/api/bucketDeleteWebsite');
const completeMultipartUpload
    = require('../../../lib/api/completeMultipartUpload');
const { config } = require('../../../lib/Config');
const constants = require('../../../constants');
const DummyRequest = require('../DummyRequest');
const initiateMultipartUpload
    = require('../../../lib/api/initiateMultipartUpload');
const { cleanup, createAlteredRequest, DummyRequestLogger, makeAuthInfo }
    = require('../helpers');
const listMultipartUploads = require('../../../lib/api/listMultipartUploads');
const listParts = require('../../../lib/api/listParts');
const metadata = require('../metadataswitch');
const multipartDelete = require('../../../lib/api/multipartDelete');
const objectDelete = require('../../../lib/api/objectDelete');
const objectGet = require('../../../lib/api/objectGet');
const objectGetACL = require('../../../lib/api/objectGetACL');
const objectHead = require('../../../lib/api/objectHead');
const objectPut = require('../../../lib/api/objectPut');
const objectPutACL = require('../../../lib/api/objectPutACL');
const objectPutPart = require('../../../lib/api/objectPutPart');
const { parseString } = require('xml2js');

const serviceGet = require('../../../lib/api/serviceGet');

const log = new DummyRequestLogger();
const accessKey = 'accessKey1';
const authInfo = makeAuthInfo(accessKey);
const canonicalID = authInfo.getCanonicalID();
const otherAccountAuthInfo = makeAuthInfo('accessKey2');
const namespace = 'default';
const usersBucketName = constants.usersBucket;
const bucketName = 'bucketname';
const baseTestRequest = {
    bucketName,
    namespace,
    url: '/',
    post: '',
    headers: { host: `${bucketName}.s3.amazonaws.com` },
};
const serviceGetRequest = {
    parsedHost: 's3.amazonaws.com',
    headers: { host: 's3.amazonaws.com' },
    url: '/',
};

const userBucketOwner = 'admin';
const creationDate = new Date().toJSON();
const usersBucket = new BucketInfo(usersBucketName,
    userBucketOwner, userBucketOwner, creationDate);
const locationConstraint = 'us-east-1';

describe('transient bucket handling', () => {
    beforeEach(done => {
        cleanup();
        const bucketMD = new BucketInfo(bucketName, canonicalID,
            authInfo.getAccountDisplayName(), creationDate);
        bucketMD.addTransientFlag();
        bucketMD.setSpecificAcl(otherAccountAuthInfo.getCanonicalID(),
            'WRITE_ACP');
        bucketMD.setLocationConstraint(locationConstraint);
        metadata.createBucket(bucketName, bucketMD, log, () => {
            metadata.createBucket(usersBucketName, usersBucket, log, () => {
                done();
            });
        });
    });

    test('putBucket request should complete creation of transient bucket if ' +
        'request is from same account that originally put', done => {
        bucketPut(authInfo, baseTestRequest, log, err => {
            assert.ifError(err);
            serviceGet(authInfo, serviceGetRequest, log, (err, data) => {
                parseString(data, (err, result) => {
                    expect(result.ListAllMyBucketsResult
                        .Buckets[0].Bucket.length).toBe(1);
                    expect(result.ListAllMyBucketsResult
                        .Buckets[0].Bucket[0].Name[0]).toBe(bucketName);
                    done();
                });
            });
        });
    });

    test('putBucket request should return error if ' +
        'transient bucket created by different account', done => {
        bucketPut(otherAccountAuthInfo, baseTestRequest, log, err => {
            assert.deepStrictEqual(err, errors.BucketAlreadyExists);
            serviceGet(otherAccountAuthInfo, serviceGetRequest,
                log, (err, data) => {
                    parseString(data, (err, result) => {
                        expect(result.ListAllMyBucketsResult
                        .Buckets[0]).toBe('');
                        done();
                    });
                });
        });
    });

    test('ACLs from clean up putBucket request should overwrite ACLs from ' +
        'original failed request that resulted in transient state', done => {
        const alteredRequest = createAlteredRequest({
            'x-amz-acl': 'public-read' }, 'headers',
            baseTestRequest, baseTestRequest.headers);
        bucketPut(authInfo, alteredRequest, log, err => {
            assert.ifError(err);
            metadata.getBucket(bucketName, log, (err, data) => {
                expect(data._transient).toBe(false);
                expect(data._acl.Canned).toBe('public-read');
                expect(data._owner).toBe(authInfo.getCanonicalID());
                done();
            });
        });
    });

    test('putBucketACL request should complete creation of transient bucket if ' +
        'request is from same account that originally put', done => {
        const putACLRequest = createAlteredRequest({
            'x-amz-acl': 'public-read' }, 'headers',
            baseTestRequest, baseTestRequest.headers);
        putACLRequest.url = '/?acl';
        putACLRequest.query = { acl: '' };
        bucketPutACL(authInfo, putACLRequest, log, err => {
            assert.ifError(err);
            metadata.getBucket(bucketName, log, (err, data) => {
                expect(data._transient).toBe(false);
                expect(data._acl.Canned).toBe('public-read');
                expect(data._owner).toBe(authInfo.getCanonicalID());
                done();
            });
        });
    });

    test('putBucketACL request should complete creation of transient bucket if ' +
        'request is from another authorized account', done => {
        const putACLRequest = createAlteredRequest({
            'x-amz-acl': 'public-read' }, 'headers',
            baseTestRequest, baseTestRequest.headers);
        bucketPutACL(otherAccountAuthInfo, putACLRequest, log, err => {
            assert.ifError(err);
            metadata.getBucket(bucketName, log, (err, data) => {
                expect(data._transient).toBe(false);
                expect(data._acl.Canned).toBe('public-read');
                expect(data._owner).toBe(authInfo.getCanonicalID());
                done();
            });
        });
    });

    describe('objectPut on a transient bucket', () => {
        const objName = 'objectName';
        afterAll(done => {
            metadata.deleteObjectMD(bucketName, objName, {}, log, () => {
                done();
            });
        });

        test(
            'objectPut request should complete creation of transient bucket',
            done => {
                const setUpRequest = createAlteredRequest({}, 'headers',
                baseTestRequest, baseTestRequest.headers);
                setUpRequest.objectKey = objName;
                const postBody = Buffer.from('I am a body', 'utf8');
                const md5Hash = crypto.createHash('md5');
                const etag = md5Hash.update(postBody).digest('hex');
                const putObjRequest = new DummyRequest(setUpRequest, postBody);
                objectPut(authInfo, putObjRequest, undefined, log, err => {
                    assert.ifError(err);
                    metadata.getBucket(bucketName, log, (err, data) => {
                        expect(data._transient).toBe(false);
                        expect(data._owner).toBe(authInfo.getCanonicalID());
                        metadata.getObjectMD(bucketName, objName, {}, log,
                            (err, obj) => {
                                assert.ifError(err);
                                expect(obj['content-md5']).toBe(etag);
                                done();
                            });
                    });
                });
            }
        );
    });

    describe('initiateMultipartUpload on a transient bucket', () => {
        const objName = 'objectName';
        afterAll(done => {
            metadata.deleteObjectMD(`${constants.mpuBucketPrefix}` +
                `${bucketName}`, objName, {}, log, () => {
                    metadata.deleteBucket(`${constants.mpuBucketPrefix}` +
                        `${bucketName}`, log, () => {
                            done();
                        });
                });
        });

        test('initiateMultipartUpload request should complete ' +
            'creation of transient bucket', done => {
            const initiateRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
            initiateRequest.objectKey = objName;
            initiateRequest.url = `/${objName}?uploads`;
            initiateMultipartUpload(authInfo, initiateRequest, log, err => {
                assert.ifError(err);
                metadata.getBucket(bucketName, log, (err, data) => {
                    expect(data._transient).toBe(false);
                    expect(data._owner).toBe(authInfo.getCanonicalID());
                    metadata.listObject(`${constants.mpuBucketPrefix}` +
                        `${bucketName}`,
                        { prefix: `overview${constants.splitter}${objName}` },
                        log, (err, results) => {
                            assert.ifError(err);
                            expect(results.Contents.length).toBe(1);
                            done();
                        });
                });
            });
        });
    });

    test('deleteBucket request should delete transient bucket if ' +
        'request is from owner', done => {
        bucketDelete(authInfo, baseTestRequest, log, err => {
            assert.ifError(err);
            metadata.getBucket(bucketName, log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
        });
    });

    test('deleteBucket request should return error if ' +
        'request is not from owner', done => {
        bucketDelete(otherAccountAuthInfo, baseTestRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.AccessDenied);
                done();
            });
    });

    test('bucketGet request on transient bucket should return NoSuchBucket' +
        'error', done => {
        const bucketGetRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        bucketGetRequest.url = `/${bucketName}`;
        bucketGetRequest.query = {};
        bucketGet(authInfo, bucketGetRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
    });

    test('bucketGetACL request on transient bucket should return NoSuchBucket' +
        'error', done => {
        const bucketGetACLRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        bucketGetACLRequest.url = '/?acl';
        bucketGetACLRequest.query = { acl: '' };
        bucketGetACL(authInfo, bucketGetACLRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
    });

    test('bucketGetCors request on transient bucket should return ' +
        'NoSuchBucket error', done => {
        bucketGetCors(authInfo, baseTestRequest, log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    test('bucketPutCors request on transient bucket should return ' +
        'NoSuchBucket error', done => {
        const bucketPutCorsRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        bucketPutCorsRequest.post = '<CORSConfiguration><CORSRule>' +
        '<AllowedMethod>PUT</AllowedMethod>' +
        '<AllowedOrigin>http://www.example.com</AllowedOrigin>' +
        '</CORSRule></CORSConfiguration>';
        bucketPutCorsRequest.headers['content-md5'] = crypto.createHash('md5')
            .update(bucketPutCorsRequest.post, 'utf8').digest('base64');
        bucketPutCors(authInfo, bucketPutCorsRequest, log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    test('bucketDeleteCors request on transient bucket should return ' +
        'NoSuchBucket error', done => {
        bucketDeleteCors(authInfo, baseTestRequest, log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    test('bucketGetWebsite request on transient bucket should return ' +
        'NoSuchBucket error', done => {
        bucketGetWebsite(authInfo, baseTestRequest, log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    test('bucketPutWebsite request on transient bucket should return ' +
        'NoSuchBucket error', done => {
        const bucketPutWebsiteRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        bucketPutWebsiteRequest.post = '<WebsiteConfiguration>' +
        '<IndexDocument><Suffix>index.html</Suffix></IndexDocument>' +
        '</WebsiteConfiguration>';
        bucketPutWebsite(authInfo, bucketPutWebsiteRequest, log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    test('bucketDeleteWebsite request on transient bucket should return ' +
        'NoSuchBucket error', done => {
        bucketDeleteWebsite(authInfo, baseTestRequest, log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    test('bucketHead request on transient bucket should return NoSuchBucket' +
        'error', done => {
        bucketHead(authInfo, baseTestRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
    });

    test('completeMultipartUpload request on transient bucket should ' +
        'return NoSuchUpload error', done => {
        const completeMpuRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        const uploadId = '5555';
        completeMpuRequest.objectKey = 'objectName';
        completeMpuRequest.query = { uploadId };
        completeMultipartUpload(authInfo, completeMpuRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchUpload);
                done();
            });
    });

    test('listParts request on transient bucket should ' +
        'return NoSuchUpload error', done => {
        const listRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        const uploadId = '5555';
        listRequest.objectKey = 'objectName';
        listRequest.query = { uploadId };
        listParts(authInfo, listRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchUpload);
                done();
            });
    });

    describe('multipartDelete request on a transient bucket', () => {
        const deleteRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        const uploadId = '5555';
        deleteRequest.objectKey = 'objectName';
        deleteRequest.query = { uploadId };
        const originalLegacyAWSBehavior =
            config.locationConstraints[locationConstraint].legacyAwsBehavior;

        afterAll(done => {
            config.locationConstraints[locationConstraint].legacyAwsBehavior =
                originalLegacyAWSBehavior;
            done();
        });

        test(
            'should return NoSuchUpload error if legacyAwsBehavior is enabled',
            done => {
                config.locationConstraints[locationConstraint].
                    legacyAwsBehavior = true;
                multipartDelete(authInfo, deleteRequest, log, err => {
                    assert.deepStrictEqual(err, errors.NoSuchUpload);
                    done();
                });
            }
        );

        test('should return no error if legacyAwsBehavior is not enabled', done => {
            config.locationConstraints[locationConstraint].
                legacyAwsBehavior = false;
            multipartDelete(authInfo, deleteRequest, log, err => {
                expect(err).toBe(null);
                return done();
            });
        });
    });

    test('objectPutPart request on transient bucket should ' +
        'return NoSuchUpload error', done => {
        const putPartRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        const uploadId = '5555';
        putPartRequest.objectKey = 'objectName';
        putPartRequest.query = {
            uploadId,
            partNumber: '1' };
        objectPutPart(authInfo, putPartRequest, undefined,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchUpload);
                done();
            });
    });

    test('list multipartUploads request on transient bucket should ' +
        'return NoSuchBucket error', done => {
        const listRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        listRequest.query = {};
        listMultipartUploads(authInfo, listRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
    });

    test('objectGet request on transient bucket should' +
        'return NoSuchBucket error', done => {
        objectGet(authInfo, baseTestRequest, false,
        log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    test('objectGetACL request on transient bucket should return ' +
        'NoSuchBucket error', done => {
        objectGetACL(authInfo, baseTestRequest,
        log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    test('objectHead request on transient bucket should return ' +
        'NoSuchBucket error', done => {
        objectHead(authInfo, baseTestRequest,
        log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    test('objectPutACL request on transient bucket should return ' +
        'NoSuchBucket error', done => {
        objectPutACL(authInfo, baseTestRequest,
        log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    test('objectDelete request on transient bucket should return ' +
        'NoSuchBucket error', done => {
        objectDelete(authInfo, baseTestRequest,
        log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });
});

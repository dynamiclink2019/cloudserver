const assert = require('assert');
const { errors } = require('arsenal');

const bucketHead = require('../../../lib/api/bucketHead');
const { bucketPut } = require('../../../lib/api/bucketPut');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const namespace = 'default';
const bucketName = 'bucketname';
const testRequest = {
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};
describe('bucketHead API', () => {
    beforeEach(() => {
        cleanup();
    });

    test('should return an error if the bucket does not exist', done => {
        bucketHead(authInfo, testRequest, log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    test('should return an error if user is not authorized', done => {
        const otherAuthInfo = makeAuthInfo('accessKey2');
        bucketPut(otherAuthInfo, testRequest, log, () => {
            bucketHead(authInfo, testRequest, log, err => {
                assert.deepStrictEqual(err, errors.AccessDenied);
                done();
            });
        });
    });

    test(
        'should return no error if bucket exists and user is authorized',
        done => {
            bucketPut(authInfo, testRequest, log, () => {
                bucketHead(authInfo, testRequest, log, err => {
                    expect(err).toBe(null);
                    done();
                });
            });
        }
    );
});

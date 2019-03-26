const assert = require('assert');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const objectName = 'someObject';
const firstPutMetadata = {
    firstput: 'firstValue',
    firstputagain: 'firstValue',
    evenmoreonfirst: 'stuff',
};
const secondPutMetadata = {
    secondput: 'secondValue',
    secondputagain: 'secondValue',
};


describe('Put object with same key as prior object', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let bucketName;

        beforeAll(done => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            bucketUtil.createRandom(1)
                      .then(created => {
                          bucketName = created;
                          done();
                      })
                      .catch(done);
        });

        beforeEach(() => s3.putObjectAsync({
            Bucket: bucketName,
            Key: objectName,
            Body: 'I am the best content ever',
            Metadata: firstPutMetadata,
        }).then(() =>
            s3.headObjectAsync({ Bucket: bucketName, Key: objectName })
        ).then(res => {
            assert.deepStrictEqual(res.Metadata, firstPutMetadata);
        }));

        afterEach(() => bucketUtil.empty(bucketName));

        afterAll(() => bucketUtil.deleteOne(bucketName));

        test(
            'should overwrite all user metadata and data on overwrite put',
            () => s3.putObjectAsync({
                Bucket: bucketName,
                Key: objectName,
                Body: 'Much different',
                Metadata: secondPutMetadata,
            }).then(() =>
                s3.getObjectAsync({ Bucket: bucketName, Key: objectName })
            ).then(res => {
                assert.deepStrictEqual(res.Metadata, secondPutMetadata);
                assert.deepStrictEqual(res.Body.toString(),
                    'Much different');
            })
        );
    });
});

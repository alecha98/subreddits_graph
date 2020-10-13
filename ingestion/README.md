1) Install aws CLI and configure it with access keys

2)

 curl -O https://files.pushshift.io/reddit/comments/RC_2017-11.xz
 curl -O https://files.pushshift.io/reddit/comments/RC_2017-12.xz
3)
 unxz RC_2017-11.xz 
 unxz RC_2017-12.xz
4) 
 aws s3 cp RC_2017-11  s3://indightreddit
 aws s3 cp RC_2017-12  s3://indightreddit




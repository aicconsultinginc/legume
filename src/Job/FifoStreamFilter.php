<?php

namespace Legume\Job;

use php_user_filter;

class FifoStreamFilter extends php_user_filter
{
    public function filter($in, $out, &$consumed, $closing)
    {
        while ($bucket = stream_bucket_make_writeable($in)) {
            $pid = posix_getpid();
            $data = $bucket->data;

            var_dump($pid, $data);
        }

        return PSFS_PASS_ON;
    }
}

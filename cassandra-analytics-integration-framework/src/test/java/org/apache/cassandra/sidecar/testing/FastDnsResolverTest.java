package org.apache.cassandra.sidecar.testing;

import java.net.UnknownHostException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FastDnsResolverTest
{
    @Test
    void testResolve() throws UnknownHostException
    {
        IntegrationTestBase.FastDnsResolver resolver = new IntegrationTestBase.FastDnsResolver();
        assertThat(resolver.resolve("localhost")).isEqualTo("127.0.0.1");
        assertThat(resolver.resolve("localhost2")).isEqualTo("127.0.0.2");
        assertThat(resolver.resolve("localhost20")).isEqualTo("127.0.0.20");
        assertThat(resolver.resolve("127.0.0.2")).isEqualTo("127.0.0.2");
    }

    @Test
    void testReverseResolve() throws UnknownHostException
    {
        IntegrationTestBase.FastDnsResolver resolver = new IntegrationTestBase.FastDnsResolver();
        assertThat(resolver.reverseResolve("127.0.0.1")).isEqualTo("localhost");
        assertThat(resolver.reverseResolve("127.0.0.2")).isEqualTo("localhost2");
        assertThat(resolver.reverseResolve("127.0.0.20")).isEqualTo("localhost20");
        assertThat(resolver.reverseResolve("localhost20")).isEqualTo("localhost20");
    }
}

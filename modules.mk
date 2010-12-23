mod_sflow.la: mod_sflow.slo
	$(SH_LINK) -rpath $(libexecdir) -module -avoid-version  mod_sflow.lo
DISTCLEAN_TARGETS = modules.mk
shared =  mod_sflow.la

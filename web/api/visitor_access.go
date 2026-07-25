package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/pkg/rpc"
	"github.com/komari-monitor/komari/utils/visitorsecurity"
)

func VisitorIPBlockMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		principal := GetPrincipal(c)
		if principal != nil && principal.HasRole(rpc.RoleAdmin) {
			c.Next()
			return
		}
		if !visitorsecurity.IsBlocked(c.ClientIP()) {
			c.Next()
			return
		}

		if strings.HasPrefix(c.Request.URL.Path, "/api") {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "access denied"})
			return
		}
		c.AbortWithStatus(http.StatusForbidden)
	}
}
